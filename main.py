from neo4j import GraphDatabase

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

if __name__ == "__main__":
    create_initial_constraints()
    import_coq_files()

# Example query
"""
MATCH 
(XXX)-[:ns1__coqHasMainConclusion]->(dummy)-[:ns1__coqTarget]->(t{uri:"cic:/Coq/Init/Logic/eq.ind#0"}), 
(XXX)-[:ns1__coqHasInConclusion]->(dummy1)-[:ns1__coqTarget]->(t2{uri:"cic:/Coquelicot/Rbar/Rbar_plus.con"}) 
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