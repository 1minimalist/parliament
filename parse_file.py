import pandas as pd
from neo4j import GraphDatabase

df = pd.read_csv('gb_parliament.csv')#, skiprows=1)

# person = df[['id','name','sort_name','email']]
# organization = df[['group_id','group']]
# membership = df[['id','group_id']]
nat = []
# person = person.drop_duplicates(subset=['id'])
df = df.drop_duplicates(subset=['id','group_id'])
for i in range(df.shape[0]):
    nat.append('GB')
df = df[['id','name','sort_name','email','group_id','group']]
df.rename(columns={'group':'gname'}, inplace=True)
df['nationality'] = nat
# print(df[:2])

driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "streams"))

def add_person(tx, df):
    for index, row in df.iterrows():
        # tx.run(" CREATE (a:Person {id: $id, name: $name, sort_name: $sort_name, email: $email, nationality: $nationality}) "
        #        " CREATE (b:Organization {group_id: $group_id, gname: $group}) ",
        #    parameters = {'id': row['id'], 'name': row['name'], 'sort_name': row['sort_name'], 'email': row['email'], 'nationality': row['nationality'],
        #                  'group_id': row['group_id'], 'gname': row['gname']} )
        tx.run('''
       MERGE (a:Person {id: $id, name: $name, sort_name: $sort_name, email: $email, nationality: $nationality})
       MERGE (b:Organization {group_id: $group_id, gname: $group})
       MERGE (a.id)-[r:ISIN]->(b.group_id)
       ''', parameters = {'id': row['id'], 'name': row['name'], 'sort_name': row['sort_name'], 'email': row['email'], 'nationality': row['nationality'],
                         'group_id': row['group_id'], 'gname': row['gname']})
    tx.commit()

# def add_org(tx, df):
#     for index, row in df.iterrows():
#         tx.run(" CREATE (b:Organization {group_id: $group_id, name: $group}) ",
#            parameters = {'group_id': row['group_id'], 'group': row['group']})

# def add_mem(tx, df):
#     for index, row in df.iterrows():
#         tx.run(" MERGE (a:Membership {id:$id})-[:ISIN]->(b:Membership {group_id:$group_id}) ",
#         parameters = {'id': row['id'], 'group_id': row['group_id']})

with driver.session() as session:
    session.write_transaction(add_person, df)
#     session.write_transaction(add_org, organization)
#     session.write_transaction(add_mem, membership)

driver.close()