import os
import subprocess
import contextlib
import py2neo as pn
import tqdm
import re


UNZIP_COMMAND = r'"C:\Program Files\7-Zip\7z.exe" e {} -o{} *.rdf'
IMPORT_FILE = "imported.txt"


def unzip_everything(force):
    for location, _, files in os.walk("."):
        for file in files:
            full_path = os.path.join(location, file)
            if not full_path.endswith(".xz"):
                continue
            if os.path.exists(full_path[:-3]):
                if force:
                    os.remove(full_path[:-3])
                else:
                    continue
            command = UNZIP_COMMAND.format(full_path, location)
            print(command)
            subprocess.call(command)


@contextlib.contextmanager
def transaction_execution(graph: pn.Graph):
    transaction = graph.begin()
    yield transaction
    graph.commit(transaction)  # type: ignore


def postprocess_name(qualified_name):
    if "?" in qualified_name:
        module, name = qualified_name.split("?")
    else:
        module, name = qualified_name, qualified_name
    modules = []
    for i, c in enumerate(module):
        if c == ".":
            modules.append(module[:i])
    modules.append(module)  # the whole
    return modules, name


def process_file(file: str):
    description_pattern = re.compile(r"<rdf:Description.+?</rdf:Description>", flags=re.DOTALL)
    with open(file, encoding="utf-8") as f:
        content = f.read()
    nodes = {}  # id: (name, label)
    edges = {}  #: (id from, id to, type): count
    all_modules = set()
    e_types = [
        "defines",
        "instance-of",
        "justifies",
        "sourceref",
        "specified-in",
        "specifies",
        "uses",
    ]
    edge_tag_part = "|".join(e_types)
    # https://isabelle.in.tum.de is not necessarily the first part
    # e.g., https://cds.omdoc.org/urtheories
    url_part = "https://.+?[?](.+?)([|](.+?))?"
    pattern_about = re.compile(f'<rdf:Description rdf:about="{url_part}">')
    edge_pattern = re.compile(f'<ulo:({edge_tag_part}) rdf:resource="{url_part}"')
    for match in description_pattern.finditer(content):
        everything = match.group(0)
        is_theory = "<ulo:theory/>" in everything
        # Process this node
        about_match = pattern_about.search(everything)
        name = about_match.group(1)
        node_type = about_match.group(3)
        if node_type is None:
            if is_theory:
                node_type = "theory"
            else:
                node_type = "unknown"
        full_name = name + "|" + node_type
        modules, name = postprocess_name(name)
        assert full_name not in nodes, (full_name, file)
        nodes[full_name] = (name, node_type)
        all_modules.update(modules)
        # Process its edges
        for e_match in edge_pattern.finditer(everything):
            e_type = e_match.group(1)
            reference_id = e_match.group(2)
            ref_type = e_match.group(3)
            if ref_type is not None:
                reference_id += ref_type
            e_key = (full_name, reference_id, e_type)
            edges[e_key] = edges.get(e_key, 0) + 1
    return nodes, edges, all_modules


def update_structures(all_nodes, nodes, all_edges, edges, all_modules, modules):
    # for node, value in nodes.items():
    #     if node in all_nodes:
    #         raise ValueError(f"Overlap of nodes: {node}")
    #     all_nodes[node] = value
    all_nodes.update(nodes)
    for edge, value in edges.items():
        # if edge in all_edges:
        #     raise ValueError(f"Overlap of edges: {edge}")
        all_edges[edge] = all_edges.get(edge, 0) + value
    all_modules.update(modules)


def process_files():
    def load_imported():
        so_far = set()
        if os.path.exists(IMPORT_FILE):
            with open(IMPORT_FILE, encoding="utf-8") as f:
                so_far = eval(f.readline())
        return so_far

    known = load_imported()
    all_nodes = {}
    all_edges = {}
    all_modules = set()
    dump_file = "isabelle{}.txt"
    try:
        count = 0
        for location, _, files in tqdm.tqdm(os.walk(".")):
            for file in files:
                full_path = os.path.join(location, file)
                if full_path in known or not full_path.endswith(".rdf"):
                    continue
                nodes, edges, modules = process_file(full_path)
                update_structures(
                    all_nodes, nodes, all_edges, edges, all_modules, modules
                )
                known.add(full_path)
                count += 1
                if count % 100 == 0:
                    print(count)
                    with open(IMPORT_FILE, "w", encoding="utf-8") as f:
                        print(known, file=f)
    finally:
        with open(IMPORT_FILE, "w", encoding="utf-8") as f:
            print(known, file=f)
        n = 0
        while os.path.exists(dump_file.format(n)):
            n += 1
        dump_file = dump_file.format(n)
        with open(dump_file, "w", encoding="utf-8") as f:
            print("@@nodes", file=f)
            for key, value in all_nodes.items():
                print(f"{key}@@{value}", file=f)
            print("@@edge", file=f)
            for key, value in all_edges.items():
                print(f"{key}@@{value}", file=f)
            print("@@modules", file=f)
            print(all_modules, file=f)


def correct_isabelle(file):
    """Adds missing @@edge into the file :)"""
    with open(file, encoding="utf-8") as f, open(file + ".out", "w", encoding="utf-8") as g:
        prev_line = ""
        for line in tqdm.tqdm(f):
            if line.startswith("("):
                if prev_line.startswith("(") or prev_line.startswith("@@edge"):
                    pass
                    # nothing to do here
                else:
                    print("inserted")
                    print("@@edge", file=g)
            print(line, file=g, end="")
            prev_line = line



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


# unzip_everything(True)
# execute_cypher()
# process_files()
# a, b, c = process_file(r"C:\Users\matej\git\dagstuhl-afp-neo4j\AFP-master-rdf\rdf\Algebraic_Numbers\Algebraic_Numbers.Compare_Complex.rdf")
