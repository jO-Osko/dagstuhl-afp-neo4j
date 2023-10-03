import os
import subprocess
import contextlib
import py2neo as pn
import tqdm


UNZIP_COMMAND = r'"C:\Program Files\7-Zip\7z.exe" e {} -o{} *.rdf'
IMPORT_FILE = "imported.txt"


def unzip_everything():
    for location, _, files in os.walk("."):
        for file in files:
            full_path = os.path.join(location, file)
            if not full_path.endswith(".xz"):
                continue
            if os.path.exists(full_path[:-3]):
                continue
            command = UNZIP_COMMAND.format(full_path, location)
            print(command)
            subprocess.call(command)


@contextlib.contextmanager
def transaction_execution(graph: pn.Graph):
    transaction = graph.begin()
    yield transaction
    graph.commit(transaction)  # type: ignore


def post_process_file(file: str):
    import re

    def replacement(match):
        return (
            match.group(0)
            .replace("&", "__amp__")
            .replace(";", "__semicol__")
            .replace("^", "__pow__")
            .replace("(", "__paropen__")
            .replace(")", "__parclose__")
            .replace(" ", "__sep__")
        )

    with open(file, encoding="utf-8") as f:
        inhalten = f.read()

    inhalten = inhalten.replace("|", "__pipe__").replace("\\", "__backslash__")
    inhalten = re.sub('"https([^"]+)', replacement, inhalten)
    with open(file, "w", encoding="utf-8") as f:
        f.write(inhalten)


def execute_cypher():
    def load_imported():
        so_far = set()
        if os.path.exists(IMPORT_FILE):
            with open(IMPORT_FILE, encoding="utf-8") as f:
                so_far = eval(f.readline())
        return so_far

    known = load_imported()
    path_to_neo = "bolt://localhost:7687"
    authentication = ("neo4j", "testtest")
    g = pn.Graph(path_to_neo, auth=authentication)
    try:
        count = 0
        for location, _, files in tqdm.tqdm(os.walk(".")):
            for file in files:
                full_path = os.path.join(location, file)
                if full_path in known or not full_path.endswith(".rdf"):
                    continue
                print(full_path)
                post_process_file(full_path)
                file_abs = os.path.abspath(full_path).replace("\\", "/")
                answer = g.call(
                    "n10s.rdf.import.fetch", f"file:///{file_abs}", "RDF/XML"
                )
                something = list(answer)[0]
                status = something[0]
                reason = something[4]
                if status != "OK":
                    print(
                        f"File {full_path} caused status {status} due to reason '{reason}'"
                    )
                else:
                    known.add(full_path)
                if count % 10 == 0:
                    with open(IMPORT_FILE, "w", encoding="utf-8") as f:
                        print(known, file=f)
    finally:
        with open(IMPORT_FILE, "w", encoding="utf-8") as f:
            print(known, file=f)


execute_cypher()
