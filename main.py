from neo4j import GraphDatabase
import json
import csv

# creates pokemon node
def pokemon_node(tx, id, name, url, height, weight, types):
    tx.run(
        "CREATE (p:Pokemon {id: $id, name: $name, url: $url, height: $height, weight: $weight, types: $types})",
        id=id,
        name=name,
        url=url,
        height=height,
        weight=weight,
        types=types
    )

# creates move node
def mv_node(tx, name, description, url):
    tx.run(
        "CREATE (m:Move {name: $name, description: $description, url: $url})",
        name=name,
        description=description,
        url=url
    )

# creates evolution relationship
def evolution_rel(tx, pid1, pid2):
    tx.run("""
        MATCH (a:Pokemon {id: $pid1}), (b:Pokemon {id:$pid2})
        CREATE (a)-[:EVOLVE]->(b)
        """, 
        pid1=pid1,
        pid2=pid2
    )

# creates pokemon->move relationship
def mv_rel(tx, pid, mv_name):
    tx.run("""
        MATCH (p:Pokemon {id: $pid}), (m:Move {name:$mv_name})
        CREATE (p)-[:SKILL]->(m)
        """, 
        pid=pid,
        mv_name=mv_name
    )

# creates pokemon type strength relationship
def effectiveness_rel(tx, type1, type2):
    tx.run("""
        MATCH (a:Pokemon), (b:Pokemon)
        WHERE $type1 IN a.types AND $type2 IN b.types
        CREATE (a)-[:EFFECTIVE_AGAINST {strong: $type1, weak: $type2}]->(b)
        """,
        type1=type1,
        type2=type2
    )

# build graph database
def build(driver):
    with driver.session() as session:
        with open("pokemon_data.json") as file:
            pokemons = json.load(file)
            # create pokemon nodes
            for pokemon in pokemons:
                session.execute_write(
                    pokemon_node, 
                    pokemon["id"], 
                    pokemon["name"], 
                    pokemon["url"], 
                    pokemon["height"],
                    pokemon["weight"],
                    pokemon["types"].split(",")
                )
            
            # create pokemon->pokemon evolution relationship
            for pokemon in pokemons:
                for evo in pokemon["evolution"]:
                    session.execute_write(
                        evolution_rel, 
                        pokemon["id"], 
                        int(evo["number"][1:])
                    )
        
        # create type effectiveness relationship
        type_effectiveness = {
            "Fighting": ["Normal", "Rock", "Steel", "Ice", "Dark"],
            "Flying": ["Fighting", "Bug", "Grass"],
            "Poison": ["Grass", "Fairy"],
            "Ground": ["Poison", "Rock", "Steel", "Fire", "Eletric"],
            "Rock": ["Flying", "Bug", "Fire"],
            "Bug": ["Grass", "Psychic", "Dark"],
            "Ghost": ["Ghost", "Psychic", "Dark"],
            "Steel": ["Rock", "Ice", "Fairy"],
            "Fire": ["Bug", "Steel", "Grass", "Ice"],
            "Water": ["Ground", "Rock", "Fire"],
            "Grass": ["Ground", "Rock", "Water"],
            "Eletric": ["Flying", "Water"],
            "Psychic": ["Fighting", "Poison"],
            "Ice": ["Flying", "Ground", "Grass", "Dragon"],
            "Dragon": ["Dragon"],
            "Dark": ["Ghost", "Psychic"],
            "Fairy": ["Fighting", "Dragon", "Dark"]
        }    
        for t1 in type_effectiveness.keys():
            for t2 in type_effectiveness[t1]:
                session.execute_write(effectiveness_rel, t1, t2)

        # create move nodes and pokemon->move relationship
        with open("moves.csv") as file:
            moves = csv.DictReader(file)
            for mv in moves:
                session.execute_write(
                    mv_node, 
                    mv["name"], 
                    mv["description"], 
                    mv["url"]
                )
                for pid in mv["pokemons"]:
                    session.execute_write(mv_rel, pid, mv["name"])



# Quais Pokémons podem atacar um Pikachu pelo sua fraqueza (tipo que lhe pode causar mais dano) cujo o peso é mais de 10kg?
query1 ="""
        MATCH (p1:Pokemon)-[:EFFECTIVE_AGAINST]->(p2:Pokemon {name: "Pikachu"})
        WHERE toFloat(substring(p1.weight, 0, char_length(p1.weight) - 3)) > 10
        RETURN p1;
    """

# Qual a tipo mais comum de um Pokémon que é atacado pelo tipo gelo?
query2 ="""
        MATCH (p1:Pokemon)-[:EFFECTIVE_AGAINST]->(p2:Pokemon)
        WHERE "Ice" IN p1.types
        UNWIND p2.types as type
        RETURN type, count(type) as count
        ORDER BY count DESC
        LIMIT 1;
    """

# Quantas segundas e terceiras evoluções que fazem, no mínimo, um Pokémon dobrar de peso?
query3 ="""
        MATCH (p1:Pokemon)-[:EVOLVE]->(p2:Pokemon)
        WHERE toFloat(substring(p2.weight, 0, char_length(p2.weight) - 3)) >= 
            2 * toFloat(substring(p1.weight, 0, char_length(p1.weight) - 3))
        RETURN count(p2);
    """

with GraphDatabase.driver(uri, auth=(user, passwd)) as driver:
    build(driver)

    with driver.session() as session:
        res1 = session.run(query1)
        res2 = session.run(query2)
        res3 = session.run(query3)
        
        for record in res1:
            print(record)
        
        for record in res2:
            print(record)
        
        for record in res3:
            print(record)