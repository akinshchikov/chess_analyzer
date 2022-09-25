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
from time import sleep

import chess.pgn
import pandas as pd
import requests


DEFAULT_CHUNK_SIZE: int = 2 ** 30

DEFAULT_COMBINE_POSITIONS_LIMIT: int = 2

DEFAULT_WAITING_TIME: int = 2 ** 6

INITIAL_POSITION_MOVELESS_FEN: str = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0'

LICHESS_STANDARD_DATABASE_URL: str = 'https://database.lichess.org/standard/'

PROCESSING_GAMES_INVERSE_SHARE: int = 2 ** 10


def combine_monthly_csvs(positions_limit: int = DEFAULT_COMBINE_POSITIONS_LIMIT) -> None:
    """
    Combines several monthly csv-files into one.
    :param positions_limit:
    :return:
    """

    local_lichess_data: dict[str, tuple[bool, bool]] = get_local_lichess_data()

    position_counts: dict[str, int] = defaultdict(int)

    for database_id, (has_log, has_file) in local_lichess_data.items():
        if has_log:
            with open(file=f'lichess/csv/{database_id}.csv',
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
        .to_csv(path_or_buf=f'lichess/csv/lichess_popular_positions.csv',
                sep=',',
                header=False,
                index=False,
                )

    with open(file=f'lichess/logs/lichess_popular_positions_info',
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


def get_local_lichess_data() -> dict[str, tuple[bool, bool]]:
    """
    Gets local lichess data.
    :return:
    """

    checksums: dict[str, str] = get_lichess_standard_database_checksums()

    logs_files: list[str] = os.listdir('lichess/logs')

    local_lichess_data: dict[str, tuple[bool, bool]] = {}

    for filename in checksums.keys():
        database_id: str = filename[26:33]

        has_log: bool = (database_id in logs_files)

        has_file: bool = (os.path.exists(f'lichess/pgn/{filename}') and
                          os.popen(f'sha256sum lichess/pgn/{filename}').read().split()[0] == checksums[filename])

        local_lichess_data[database_id] = has_log, has_file

    return local_lichess_data


def get_moveless_fen(board: chess.Board) -> str:
    """
    Get fen of chess position without trailing number of moves.
    :param board:
    :return: moveless fen
    """

    return board.fen().rsplit(sep=' ', maxsplit=1)[0]


def process_lichess_monthly_database(database_id: str,
                                     chunk_size: int = DEFAULT_CHUNK_SIZE,
                                     positions_limit: int = 1,
                                     checksums: dict[str, str] = None,
                                     filenames_and_counts: dict[str, int] = None,
                                     ) -> None:
    """
    Processes lichess database for the given month.
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

    filename: str

    for filename in checksums.keys():
        if database_id in filename:
            break
    else:
        raise ValueError(f'{database_id} not found.')

    if not os.path.exists(f'lichess/pgn/{filename}'):
        raise OSError(f'{filename} doesn`t exist.')

    sha256sum: str = os.popen(f'sha256sum lichess/pgn/{filename}').read().split()[0]

    if sha256sum != checksums[filename]:
        raise OSError(f'{filename} has the wrong sha256sum. {sha256sum} instead of {checksums[filename]}.')

    game_elos: list[int] = [0, 0]

    elo_counter: defaultdict[int, int] = defaultdict(int)

    bz2_file: bz2.BZ2File = bz2.open(filename=f'lichess/pgn/{filename}',
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

    cumulative_elo_counter: int = 0

    min_elo: int = 0

    for elo in sorted(elo_counter, reverse=True):
        cumulative_elo_counter += elo_counter[elo]

        if cumulative_elo_counter * PROCESSING_GAMES_INVERSE_SHARE >= filenames_and_counts[filename]:
            min_elo = elo

            break

    bz2_file = bz2.open(filename=f'lichess/pgn/{filename}',
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

                game_positions: set = set()

                board: chess.Board = game.board()

                game_positions.add(get_moveless_fen(board))

                for move in game.mainline_moves():
                    board.push(move)

                    game_positions.add(get_moveless_fen(board))

                for game_position in game_positions:
                    position_counts[game_position] += 1

    position_counts_dataframe: pd.DataFrame = \
        pd.DataFrame(data=(item for item in position_counts.items() if item[1] >= positions_limit),
                     columns=('moveless_fen', f'{database_id}'),
                     )

    position_counts_dataframe.to_csv(path_or_buf=f'lichess/csv/{database_id}.csv',
                                     sep=',',
                                     header=False,
                                     index=False,
                                     )

    with open(file=f'lichess/logs/{database_id}',
              mode='w',
              encoding='utf-8',
              ) as file:
        file.write(f'{min_elo}\n{position_counts[INITIAL_POSITION_MOVELESS_FEN]}\n{len(position_counts)}\n')

    os.remove(f'lichess/pgn/{filename}')


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

    local_lichess_data: dict[str, tuple[bool, bool]] = get_local_lichess_data()

    process_dict: dict[str, mp.Process] = {}

    running_process_set: set[mp.Process] = set()

    for filename in checksums.keys():
        database_id: str = filename[26:33]

        has_log: bool

        has_file: bool

        has_log, has_file = local_lichess_data[database_id]

        if has_file and not has_log:
            process_dict[database_id] = \
                mp.Process(target=process_lichess_monthly_database,
                           kwargs={'database_id': database_id,
                                   'checksums': checksums,
                                   },
                           )

    while process_dict or running_process_set:
        for process in running_process_set:
            if not process.is_alive():
                running_process_set.remove(process)

                break

        if len(running_process_set) < threads_count:
            for database_id, process in process_dict.items():
                running_process_set.add(process)

                process.start()

                del process_dict[database_id]

                break
        else:
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
