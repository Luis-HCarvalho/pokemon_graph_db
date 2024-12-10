# DB Design

### node:Pokemon
    - id: number
    - name: string
    - url: string
    - height: string
    - weight: string
    - types: List

### node:Move
    - name: string
    - description: string
    - url: string

### relationship:SKILL   (pokemon)->(move)

### relationship:EVOLVE  (pokemon)->(pokemon)

### relationship:EFFECTIV_AGAINST   (pokemon)->(pokemon)
    - strong: string
    - weak: string

## To Run this project you must provide Neo4j credentials (username and password) by edition the main.py script
