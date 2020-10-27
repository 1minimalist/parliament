import pandas as pd
from neo4j import GraphDatabase

df = pd.read_csv('gb_parliament.csv')#, skiprows=1)

person = df[['id','name','sort_name','email']]
organization = df[['group_id','group']]
membership = df[['id','group_id']]
nat = []
person = person.drop_duplicates(subset=['id'])
for i in range(person.shape[0]):
    nat.append('GB')
person['nationality'] = nat
print(person.shape)

driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "streams"))

def add_person(tx, df):
    for index, row in df.iterrows():
        tx.run(" CREATE (a:Person {id: $id, name: $name, sort_name: $sort_name, email: $email, nationality: $nationality}) ",
           parameters = {'id': row['id'], 'name': row['name'], 'sort_name': row['sort_name'], 'email': row['email'], 'nationality': row['nationality']})

def add_org(tx, df):
    for index, row in df.iterrows():
        tx.run(" CREATE (b:Organization {group_id: $group_id, name: $group}) ",
           parameters = {'group_id': row['group_id'], 'group': row['group']})

def add_mem(tx):
    tx.run(" MATCH (a),(b) WHERE id(a) =1 and group_id(b) = 2 create (a)-[r:ISIN]->(b) RETURN a, b" )

with driver.session() as session:
    session.write_transaction(add_person, person)
    session.write_transaction(add_org, organization)
    session.write_transaction(add_mem, membership)

driver.close()