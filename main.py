from neo4j import GraphDatabase
from typing import List

import os

folder = "dag2"
def get_driver():
    return GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))


def create_initial_constraints():
    cmds = [
        "CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE",
        "CREATE INDEX library_name_resource IF NOT EXISTS FOR (r:Resource) ON r.library_name",
        "CREATE INDEX trans_name_resource IF NOT EXISTS FOR (r:Resource) ON r.trans_name",
        "call n10s.graphconfig.init()"
    ]
    with get_driver().session() as session:
        for cmd in cmds:
            try:
                session.run(cmd)
            except Exception as e:
                print(e)

def preprocess_coq_files():

    files = os.listdir(folder)
    for file in files:
        full_file = os.path.join(folder, file)
        if os.path.isfile(full_file):
            preprocess_coq_file(full_file)


def preprocess_coq_file(fname: str):
    f = open(fname, "r")
    lines = f.read()
    self_replaces = [
        ("<rdf:RDF", "xml:base=\"http://dagstuhl/coq/\"")
    ]
    for (old, new) in self_replaces:
        lines = lines.replace(old, old + " " + new)

    f.close()
    f = open(fname, "w")
    f.write(lines)
    f.close()


def import_coq_files():
    driver = get_driver()

    files = os.listdir(folder)
    for file in files:
        full_file = os.path.join(folder, file)

        if os.path.isfile(full_file):
            # print("running", full_file)
            f = open(full_file, "r")
            if "cic:/Coquelicot/Lub/is_lub_Rbar_unique.con" not in f.read():
                print("NOT FOUND", full_file)
                #continue
            # continue


            print("running", full_file)
            with driver.session() as session:
                run = session.run(
                    "call n10s.rdf.import.fetch('file:///" + os.path.join(os.getcwd(), full_file) + "', 'RDF/XML') yield terminationStatus as status return status"
                )
                print(run.to_eager_result())
                run = session.run(
                    # Bad, I know
                    f"MATCH (n:Resource) WHERE n.library_name IS NULL SET n.library_name = '{file}'"
                )
                print(run.to_eager_result())

    with driver.session() as session:
        # TODO: This should be trigger
        session.run("MATCH (n)-[:ns1__coqTarget]->(m) SET n.trans_name = m.uri").to_eager_result()
    driver.close()

def update_labels():
    labels = """
ns0__as
ns0__axiom
ns0__definition
ns0__derived
ns0__example
ns0__file
ns0__folder
ns0__library
ns0__library-group
ns0__object
ns0__predicate
ns0__primitive
ns0__proposition
ns0__revision
ns0__role
ns0__statement
ns0__theory
ns0__type
""".split("\n")
    for l in labels:
        if not l: continue
        lbl = l.replace("ns0__", "").replace("-", "_")
        assert lbl != l
        with get_driver().session() as sesion:
            print(
                sesion.run(
                    f"MATCH (r:Resource) WHERE r.`{l}` IS NOT NULL SET r:{lbl}"
                ).to_eager_result()
            )

def make_it_agda_like():

    references = [
        ("REFERENCE_BODY", ["ns0__uses"]),
        ("REFERENCE_TYPE", ["ns1__coqHasInConclusion", "ns1__coqHasInHypothesis", "ns1__coqHasMainConclusion", "ns1__coqHasMainHypothesis"]),
    ]

    for (lbl, rels) in references:
        with get_driver().session() as session:
            for rel in rels:
                print(
                    session.run(
                        f"MATCH (n)-[r:{rel}]->(m) MERGE (n)-[:{lbl}]->(m)"
                    ).to_eager_result()
                )

def prepare_search_query(main_conclusion: str, in_conclusions: List[str], required_in_conclusion: List[str]):
    TARGET_NAME = "XXX"
    TARGET_NAME2 = "ZZZ"

    initial_query = ",\n".join(
        [f'({TARGET_NAME})-[:ns1__coqHasMainConclusion]->()-[:ns1__coqTarget]->(t{{uri:"{main_conclusion}"}})'] +
        [
            f'({TARGET_NAME})-[:ns1__coqHasInConclusion]->()-[:ns1__coqTarget]->(t{i}{{uri:"{in_conclusion}"}})' for (i, in_conclusion) in enumerate(in_conclusions)
        ]
    )

    query = (f"MATCH \n"
             f"{initial_query} \n"
             f"WITH {TARGET_NAME} \n"
             f"MATCH ({TARGET_NAME})-[:ns1__coqHasInConclusion]->()-[:ns1__coqTarget]->({TARGET_NAME2})\n"
             f"WITH {TARGET_NAME}, COLLECT({TARGET_NAME2}.uri) as c\n"
             f"WHERE apoc.coll.containsAll({required_in_conclusion}, c)\n"
             f"RETURN {TARGET_NAME}.uri\n"
             )

    return query

if __name__ == "__main__":
    #create_initial_constraints()
    #import_coq_files()

    # update_labels()
    # make_it_agda_like()

    print(
        prepare_search_query(
            "cic:/Coq/Init/Logic/eq.ind#0",
            ["cic:/Coquelicot/Rbar/Rbar_plus.con"],
            ["cic:/Coquelicot/Rbar/Rbar.ind#0", "cic:/Coquelicot/Rbar/Rbar_plus.con"]
    ))

    pass

# Example query
"""
MATCH 
(XXX)-[:ns1__coqHasMainConclusion]->()-[:ns1__coqTarget]->(t{uri:"cic:/Coq/Init/Logic/eq.ind#0"}), 
(XXX)-[:ns1__coqHasInConclusion]->()-[:ns1__coqTarget]->(t2{uri:"cic:/Coquelicot/Rbar/Rbar_plus.con"}) 
WITH XXX 
MATCH (XXX)-[:ns1__coqHasInConclusion]->(dummy)-[:ns1__coqTarget]->(ZZZ)

WITH XXX, COLLECT(ZZZ.uri) as c
WHERE apoc.coll.containsAll(["cic:/Coquelicot/Rbar/Rbar_plus.con", "cic:/Coquelicot/Rbar/Rbar.ind#0"], c)
RETURN XXX.uri

"""

"""
MATCH 
(n)-[ns1__coqHasMainHypothesis]->(mHypot{uri:"cic:/Coquelicot/Lub/is_lub_Rbar.con"}),
(n)-[ns1__coqHasMainConclusion]->(mConc) // WHERE mConc.uri CONTAINS "Logic/eq.ind#0" // cic:/Coq/Init/Logic/eq.ind#0
RETURN n.uri, mConc.uri

"""


"""
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n)-[e]-(m) WITH e LIMIT 500000 DELETE e;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;
MATCH (n) WITH n LIMIT 100000 DETACH DELETE n;

"""