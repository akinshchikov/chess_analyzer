"""
    Copyright 2022 Alexey Akinshchikov
    akinshchikov@gmail.com

    This file contains functions for downloading and processing of database from lichess.org.
"""

import bz2
import io
import multiprocessing as mp
import os

from collections import defaultdict
from pathlib import Path
from time import sleep

import chess.pgn
import pandas as pd
import requests


DEFAULT_CHUNK_SIZE: int = 2 ** 30

DEFAULT_COMBINE_POSITIONS_LIMIT: int = 2

DEFAULT_WAITING_TIME: int = 2 ** 8

INITIAL_POSITION_MOVELESS_FEN: str = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0'

LICHESS_STANDARD_DATABASE_URL: str = 'https://database.lichess.org/standard/'

PROCESSING_GAMES_INVERSE_SHARE: int = 2 ** 11


def check_file_errors(checksums: dict[str, str],
                      database_id: str,
                      filename: str, ) -> None:
    """
    Checks for file-related errors.
    :param checksums:
    :param database_id:
    :param filename:
    :return:
    """

    if filename not in checksums.keys():
        raise ValueError(f'{database_id} checksum not found.')

    if not Path(f'lichess/pgn_bz2/{filename}').exists():
        raise OSError(f'{filename} doesn`t exist.')

    sha256sum: str = os.popen(f'sha256sum lichess/pgn_bz2/{filename}').read().split()[0]

    if checksums[filename] != os.popen(f'sha256sum lichess/pgn_bz2/{filename}').read().split()[0]:
        raise OSError(f'{filename} has the wrong sha256sum. {sha256sum} instead of {checksums[filename]}.')


def check_monthly_database_elo_csv_availability(checksums: dict[str, str] | None = None) -> dict[str, bool]:
    """
    Checks for every lichess.org monthly database if its elo csv is available.
    :param checksums:
    :return:
    """

    if checksums is None:
        checksums = get_lichess_standard_database_checksums()

    elo_csvs: tuple[str] = tuple(child.name for child in Path('lichess/elo_csv').iterdir())

    monthly_database_elo_csv_availability: dict[str, bool] = {}

    for filename in checksums.keys():
        database_id: str = filename[26:33]

        has_elo_csv: bool = (f'{database_id}_elo.csv' in elo_csvs)

        monthly_database_elo_csv_availability[database_id] = has_elo_csv

    return monthly_database_elo_csv_availability


def check_monthly_database_pgn_log_availability(checksums: dict[str, str] | None = None) -> dict[str, bool]:
    """
    Checks for every lichess.org monthly database if its local log is available.
    :param checksums:
    :return:
    """

    if checksums is None:
        checksums = get_lichess_standard_database_checksums()

    pgn_logs: tuple[str] = tuple(child.name for child in Path('lichess/pgn_log').iterdir())

    monthly_database_pgn_log_availability: dict[str, bool] = {}

    for filename in checksums.keys():
        database_id: str = filename[26:33]

        has_pgn_log: bool = (database_id in pgn_logs)

        monthly_database_pgn_log_availability[database_id] = has_pgn_log

    return monthly_database_pgn_log_availability


def check_monthly_database_pgn_bz2_availability(checksums: dict[str, str] | None = None) -> dict[str, bool]:
    """
    Checks for every lichess.org monthly database if its pgn file is downloaded and has correct sha256 checksum.
    :param checksums:
    :return:
    """

    if checksums is None:
        checksums = get_lichess_standard_database_checksums()

    monthly_database_pgn_bz2_availability: dict[str, bool] = {}

    filename: str

    database_id: str

    for filename in checksums.keys():
        database_id = filename[26:33]

        monthly_database_pgn_bz2_availability[database_id] = False

    sha256sum_command: str = 'parallel sha256sum ::: ' + ' '.join(
        f'lichess/pgn_bz2/{filename}' for filename in checksums.keys() if Path(f'lichess/pgn_bz2/{filename}').exists()
    )

    sha256sum_output: list[str] = os.popen(cmd=sha256sum_command).read().split()

    for index in range(len(sha256sum_output) // 2):
        sha256sum: str = sha256sum_output[index * 2]

        filename = sha256sum_output[index * 2 + 1][16:]

        database_id = filename[26:33]

        monthly_database_pgn_bz2_availability[database_id] = (sha256sum == checksums[filename])

    return monthly_database_pgn_bz2_availability


def combine_monthly_csvs(positions_limit: int = DEFAULT_COMBINE_POSITIONS_LIMIT) -> None:
    """
    Combines several monthly csv-files into one.
    :param positions_limit:
    :return:
    """

    pgn_log_availability: dict[str, bool] = check_monthly_database_pgn_log_availability()

    position_counts: dict[str, int] = defaultdict(int)

    for database_id, has_pgn_log in pgn_log_availability.items():
        if has_pgn_log:
            with open(file=f'lichess/pgn_csv/{database_id}.csv',
                      mode='r',
                      encoding='utf-8',
                      ) as file:
                for line in file:
                    moveless_fen, count = line.split(sep=',')

                    position_counts[moveless_fen] += int(count)

    position_counts_dataframe: pd.DataFrame = \
        pd.DataFrame(data=(item for item in position_counts.items() if item[1] >= positions_limit),
                     columns=('moveless_fen', 'count'),
                     )

    position_counts_dataframe \
        .sort_values(by='count',
                     ascending=False,
                     ) \
        .to_csv(path_or_buf='lichess/pgn_csv/lichess_popular_positions.csv',
                sep=',',
                header=False,
                index=False,
                )

    with open(file='lichess/pgn_log/lichess_popular_positions_info',
              mode='w',
              encoding='utf-8',
              ) as file:
        file.write(f'GAMES_COUNT:     {position_counts[INITIAL_POSITION_MOVELESS_FEN]}\n')

        file.write(f'POSITIONS_COUNT: {len(position_counts_dataframe.index)}\n')


def get_lichess_standard_database_checksums(url: str = f'{LICHESS_STANDARD_DATABASE_URL}sha256sums.txt',
                                            ) -> dict[str, str]:
    """
    Get dict of pairs (filename, sha256sum) for monthly lichess.org standard databases from corresponding URL.
    :param url:
    :return:
    """

    page_text: str = requests.get(url=url).text

    checksums: dict[str, str] = \
        {line.split()[1]: line.split()[0] for line in page_text.split('\n') if ' ' in line}

    return checksums


def get_lichess_standard_database_filenames_and_counts(url: str = f'{LICHESS_STANDARD_DATABASE_URL}counts.txt',
                                                       ) -> dict[str, int]:
    """
    Get dict of pairs (filename, games_count) for monthly lichess.org standard databases from corresponding URL.
    :param url:
    :return: filenames_and_counts
    """

    page_text: str = requests.get(url=url).text

    filenames_and_counts: dict[str, int] = \
        {line.split()[0]: int(line.split()[1]) for line in page_text.split('\n') if ' ' in line}

    return filenames_and_counts


def get_moveless_fen(board: chess.Board) -> str:
    """
    Get fen of chess position without trailing number of moves.
    :param board:
    :return: moveless fen
    """

    return board.fen().rsplit(maxsplit=1)[0]


def generate_lichess_monthly_database_elo_csv(database_id: str,
                                              chunk_size: int = DEFAULT_CHUNK_SIZE,
                                              checksums: dict[str, str] | None = None,
                                              ) -> None:
    """
    Generates elo csv from lichess database for the given month.
    :param database_id:
    :param chunk_size:
    :param checksums:
    :return:
    """

    if checksums is None:
        checksums = get_lichess_standard_database_checksums()

    filename: str = f'lichess_db_standard_rated_{database_id}.pgn.bz2'

    check_file_errors(checksums, database_id, filename)

    game_elos: list[int] = [0, 0]

    elo_counter: defaultdict[int, int] = defaultdict(int)

    bz2_file: bz2.BZ2File = bz2.open(filename=f'lichess/pgn_bz2/{filename}',
                                     mode='r',
                                     )

    while True:
        try:
            chunk: list[bytes] = bz2_file.readlines(size=chunk_size)
        except EOFError:
            chunk = bz2_file.readlines()

        if not chunk:
            break

        for line in chunk:
            update_game_elos(line=line,
                             game_elos=game_elos,
                             )

            if b'1. ' in line and b'"]' not in line:
                elo_counter[min(game_elos)] += 1

    bz2_file.close()

    elo_counter_dataframe: pd.DataFrame = pd.DataFrame(data=elo_counter.items(),
                                                       columns=('elo', 'count'),
                                                       )

    elo_counter_dataframe \
        .sort_values(by='elo',
                     ascending=True,
                     ) \
        .to_csv(path_or_buf=f'lichess/elo_csv/{database_id}_elo.csv',
                sep=',',
                header=False,
                index=False,
                )


def generate_lichess_monthly_database_pgn_csv(database_id: str,
                                              chunk_size: int = DEFAULT_CHUNK_SIZE,
                                              positions_limit: int = 1,
                                              checksums: dict[str, str] | None = None,
                                              filenames_and_counts: dict[str, int] | None = None,
                                              ) -> None:
    """
    Generates elo csv from lichess database for the given month.
    :param database_id:
    :param chunk_size:
    :param positions_limit:
    :param checksums:
    :param filenames_and_counts:
    :return:
    """

    if checksums is None:
        checksums = get_lichess_standard_database_checksums()

    if filenames_and_counts is None:
        filenames_and_counts = get_lichess_standard_database_filenames_and_counts()

    filename: str = f'lichess_db_standard_rated_{database_id}.pgn.bz2'

    check_file_errors(checksums, database_id, filename)

    game_elos: list[int] = [0, 0]

    elo_counter_dataframe = pd.read_csv(filepath_or_buffer=f'lichess/elo_csv/{database_id}_elo.csv',
                                        sep=',',
                                        header=None,
                                        names=('elo', 'count'),
                                        index_col='elo',
                                        dtype=int,
                                        )

    elo_counter: dict[int, int] = dict(elo_counter_dataframe['count'])

    cumulative_elo_counter: int = 0

    min_elo: int = 0

    for elo in sorted(elo_counter, reverse=True):
        cumulative_elo_counter += elo_counter[elo]

        if cumulative_elo_counter * PROCESSING_GAMES_INVERSE_SHARE >= filenames_and_counts[filename]:
            min_elo = elo

            break

    bz2_file: bz2.BZ2File = bz2.open(filename=f'lichess/pgn_bz2/{filename}',
                                     mode='r',
                                     )

    position_counts: defaultdict[str, int] = defaultdict(int)

    while True:
        try:
            chunk: list[bytes] = bz2_file.readlines(size=chunk_size)
        except EOFError:
            chunk = bz2_file.readlines()

        if not chunk:
            break

        for line in chunk:
            update_game_elos(line=line,
                             game_elos=game_elos,
                             )

            if b'1. ' in line and b'"]' not in line and min(game_elos) >= min_elo:
                pgn: io.StringIO = io.StringIO(line.decode("utf-8"))

                game: chess.pgn.Game = chess.pgn.read_game(pgn)

                game_positions: set[str] = set()

                board: chess.Board = game.board()

                game_positions.add(get_moveless_fen(board))

                for move in game.mainline_moves():
                    board.push(move)

                    game_positions.add(get_moveless_fen(board))

                for game_position in game_positions:
                    position_counts[game_position] += 1

    bz2_file.close()

    position_counts_dataframe: pd.DataFrame = \
        pd.DataFrame(data=(item for item in position_counts.items() if item[1] >= positions_limit),
                     columns=('moveless_fen', 'count'),
                     )

    position_counts_dataframe \
        .sort_values(by='count',
                     ascending=False,
                     ) \
        .to_csv(path_or_buf=f'lichess/pgn_csv/{database_id}.csv',
                sep=',',
                header=False,
                index=False,
                )

    with open(file=f'lichess/pgn_log/{database_id}',
              mode='w',
              encoding='utf-8',
              ) as file:
        file.write(f'{min_elo}\n{position_counts[INITIAL_POSITION_MOVELESS_FEN]}\n{len(position_counts)}\n')


def process_lichess_monthly_databases(threads_count: int = 1,
                                      waiting_time: int = DEFAULT_WAITING_TIME,
                                      ) -> None:
    """
    Processes all lichess databases.
    :param threads_count:
    :param waiting_time:
    :return:
    """

    checksums: dict[str, str] = get_lichess_standard_database_checksums()

    pgn_log_availability: dict[str, bool] = check_monthly_database_pgn_log_availability(checksums=checksums)

    elo_csv_availability: dict[str, bool] = check_monthly_database_elo_csv_availability(checksums=checksums)

    pgn_bz2_availability: dict[str, bool] = check_monthly_database_pgn_bz2_availability(checksums=checksums)

    elo_csv_process_dict: dict[str, mp.Process] = {}

    pgn_csv_process_dict: dict[str, mp.Process] = {}

    elo_csv_process_need: dict[str, bool] = {key: not value for key, value in elo_csv_availability.items()}

    pgn_csv_process_need: dict[str, bool] = {key: not value for key, value in pgn_log_availability.items()}

    running_process_set: set[mp.Process] = set()

    database_id: str

    for filename in checksums.keys():
        database_id = filename[26:33]

        if pgn_bz2_availability[database_id]:
            if not elo_csv_availability[database_id]:
                elo_csv_process_need[database_id] = False

                elo_csv_process_dict[database_id] = \
                    mp.Process(target=generate_lichess_monthly_database_elo_csv,
                               kwargs={'database_id': database_id,
                                       'checksums': checksums,
                                       },
                               name=f'{database_id} elo',
                               )
            elif not pgn_log_availability[database_id]:
                pgn_csv_process_need[database_id] = False

                pgn_csv_process_dict[database_id] = \
                    mp.Process(target=generate_lichess_monthly_database_pgn_csv,
                               kwargs={'database_id': database_id,
                                       'checksums': checksums,
                                       },
                               name=f'{database_id} pgn',
                               )

    while elo_csv_process_dict or pgn_csv_process_dict or running_process_set:
        for process in running_process_set:
            if not process.is_alive():
                running_process_set.remove(process)

                break

        while len(running_process_set) < threads_count and elo_csv_process_dict:
            for database_id, process in elo_csv_process_dict.items():
                running_process_set.add(process)

                process.start()

                del elo_csv_process_dict[database_id]

                break
        else:
            while len(running_process_set) < threads_count and pgn_csv_process_dict:
                for database_id, process in pgn_csv_process_dict.items():
                    running_process_set.add(process)

                    process.start()

                    del pgn_csv_process_dict[database_id]

                    break
            else:
                for filename, _ in checksums.items():
                    database_id = filename[26:33]

                    if not pgn_bz2_availability[database_id]:
                        if Path(f'lichess/pgn_bz2/{filename}').exists():
                            if os.popen(cmd=f'sha256sum lichess/pgn_bz2/{filename}').read().split()[0] == \
                                    checksums[filename]:
                                pgn_bz2_availability[database_id] = True

                    if not elo_csv_availability[database_id]:
                        if Path(f'lichess/elo_csv/{database_id}').exists():
                            elo_csv_availability[database_id] = True

                    if not pgn_log_availability[database_id]:
                        if Path(f'lichess/pgn_log/{database_id}').exists():
                            pgn_log_availability[database_id] = True

                    if pgn_bz2_availability[database_id]:
                        if not elo_csv_availability[database_id]:
                            if elo_csv_process_need[database_id]:
                                elo_csv_process_need[database_id] = False

                                elo_csv_process_dict[database_id] = \
                                    mp.Process(target=generate_lichess_monthly_database_elo_csv,
                                               kwargs={'database_id': database_id,
                                                       'checksums': checksums,
                                                       },
                                               name=f'{database_id} elo',
                                               )
                        elif not pgn_log_availability[database_id]:
                            if pgn_csv_process_need[database_id]:
                                pgn_csv_process_need[database_id] = False

                                pgn_csv_process_dict[database_id] = \
                                    mp.Process(target=generate_lichess_monthly_database_pgn_csv,
                                               kwargs={'database_id': database_id,
                                                       'checksums': checksums,
                                                       },
                                               name=f'{database_id} pgn',
                                               )

                        if pgn_log_availability[database_id] and elo_csv_availability[database_id]:
                            Path(f'lichess/pgn_bz2/{filename}').unlink()

                            pgn_bz2_availability[database_id] = False

        sleep(waiting_time)


def update_game_elos(line: bytes,
                     game_elos: list[int]) -> None:
    """
    Updates elos of the game players.
    :param line:
    :param game_elos:
    :return:
    """

    if b'WhiteElo' in line:
        white_elo_str: str = ''.join(filter(str.isdigit, line.decode("utf-8")))

        if white_elo_str:
            game_elos[0] = int(white_elo_str)

    if b'BlackElo' in line:
        black_elo_str: str = ''.join(filter(str.isdigit, line.decode("utf-8")))

        if black_elo_str:
            game_elos[1] = int(black_elo_str)
