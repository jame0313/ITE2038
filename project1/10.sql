SELECT Pokemon.name
FROM Pokemon
LEFT OUTER JOIN CatchedPokemon ON Pokemon.id = CatchedPokemon.pid
WHERE CatchedPokemon.owner_id is NULL
ORDER BY Pokemon.name
