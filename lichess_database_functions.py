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


INITIAL_POSITION_MOVELESS_FEN: str = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0'

LICHESS_STANDARD_DATABASE_URL: str = 'https://database.lichess.org/standard/'


def get_lichess_standard_database_checksums(url: str = f'{LICHESS_STANDARD_DATABASE_URL}sha256sums.txt',
                                            ) -> dict[str, str]:
    page_text: str = requests.get(url=url).text

    checksums: dict[str, str] = \
        {line.split()[1]: line.split()[0] for line in page_text.split('\n') if ' ' in line}

    return checksums


def get_lichess_standard_database_filenames_and_counts(url: str = f'{LICHESS_STANDARD_DATABASE_URL}counts.txt',
                                                       ) -> dict[str, int]:
    """
    Get tuple of pairs (filename, games_count) for monthly lichess.org standard databases from corresponding URL.
    :param url:
    :return: filenames_and_counts
    """

    page_text: str = requests.get(url=url).text

    filenames_and_counts: dict[str, int] = \
        {line.split()[0]: int(line.split()[1]) for line in page_text.split('\n') if ' ' in line}

    return filenames_and_counts


def get_local_lichess_data() -> dict[str, tuple[bool, int]]:
    """
    Gets local lichess data.
    :return:
    """

    filenames_and_counts: dict[str, int] = get_lichess_standard_database_filenames_and_counts()

    logs_files: list[str] = os.listdir('lichess/logs')

    csv_files: list[str] = os.listdir('lichess/csv')

    local_lichess_data: dict[str, tuple[bool, int]] = {}

    for filename in filenames_and_counts.keys():
        database_id: str = filename[26:33]

        has_log: bool = (database_id in logs_files)

        csv_count: int = sum(1 for csv_file in csv_files if database_id in csv_file)

        local_lichess_data[database_id] = has_log, csv_count

    return local_lichess_data


def get_moveless_fen(board: chess.Board) -> str:
    """
    Get fen of chess position without trailing number of moves.
    :param board:
    :return: moveless fen
    """

    return board.fen().rsplit(sep=' ', maxsplit=1)[0]


def process_lichess_database_month(database_id: str,
                                   chunk_size: int = 2 ** 30,
                                   positions_limit: int = 2 ** 3,
                                   csv_count: int = 0,
                                   filenames_and_counts: dict[str, int] = None,
                                   checksums: dict[str, str] = None,
                                   ) -> None:
    """
    Processes lichess database for given month.
    :param database_id:
    :param chunk_size:
    :param positions_limit:
    :param csv_count:
    :param filenames_and_counts:
    :param checksums:
    :return:
    """

    if filenames_and_counts is None:
        filenames_and_counts = get_lichess_standard_database_filenames_and_counts()

    if checksums is None:
        checksums = get_lichess_standard_database_checksums()

    filename: str

    games_count: int

    for filename in filenames_and_counts.keys():
        if database_id in filename:
            break
    else:
        raise ValueError(f'{database_id} not found.')

    if os.path.exists(f'lichess/pgn/{filename}'):
        if os.popen(f'sha256sum lichess/pgn/{filename}').read().split()[0] != checksums[filename]:
            os.remove(f'lichess/pgn/{filename}')

    if not os.path.exists(f'lichess/pgn/{filename}'):
        os.system(f'wget --no-verbose -O lichess/pgn/{filename} {LICHESS_STANDARD_DATABASE_URL}{filename}')

    bz2_file: bz2.BZ2File = bz2.open(filename=f'lichess/pgn/{filename}',
                                     mode='r',
                                     )

    chunk_id: int = 0

    while True:
        try:
            chunk: bytes = b''.join(line for line in bz2_file.readlines(size=chunk_size))
        except EOFError:
            chunk = b''.join(line for line in bz2_file.readlines())

        if chunk == b'':
            break

        if chunk_id >= csv_count:
            pgn: io.StringIO = io.StringIO(chunk.decode("utf-8"))

            positions: defaultdict[str, int] = defaultdict(int)

            while True:
                game: chess.pgn.Game = chess.pgn.read_game(pgn)

                if game is None:
                    break

                game_positions: set = set()

                board: chess.Board = game.board()

                game_positions.add(get_moveless_fen(board))

                for move in game.mainline_moves():
                    board.push(move)

                    game_positions.add(get_moveless_fen(board))

                for game_position in game_positions:
                    positions[game_position] += 1

            df: pd.DataFrame = pd.DataFrame(data=(item for item in positions.items() if item[1] >= positions_limit),
                                            columns=('moveless_fen', f'{database_id}_{chunk_id}'),
                                            )

            df.to_csv(path_or_buf=f'lichess/csv/{database_id}_{chunk_id}.csv',
                      sep=',',
                      header=False,
                      index=False,
                      )

        chunk_id += 1

    with open(file=f'lichess/logs/{database_id}',
              mode='w',
              encoding='utf-8',
              ) as file:
        file.write(f'{database_id}\n')

    os.remove(f'lichess/pgn/{filename}')


def process_lichess_databases(threads_count: int = 1) -> None:
    """
    Processes all lichess databases.
    :return:
    """

    filenames_and_counts: dict[str, int] = get_lichess_standard_database_filenames_and_counts()

    checksums: dict[str, str] = get_lichess_standard_database_checksums()

    local_lichess_data: dict[str, tuple[bool, int]] = get_local_lichess_data()

    process_dict: dict[str, mp.Process] = {}

    running_process_set: set[mp.Process] = set()

    for filename in filenames_and_counts.keys():
        database_id: str = filename[26:33]

        has_log: bool

        csv_count: int

        has_log, csv_count = local_lichess_data[database_id]

        if not has_log:
            process_dict[database_id] = \
                mp.Process(target=process_lichess_database_month,
                           kwargs={'database_id': database_id,
                                   'csv_count': csv_count,
                                   'filenames_and_counts': filenames_and_counts,
                                   'checksums': checksums,
                                   },
                           )

    while process_dict:
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
            sleep(10.)
