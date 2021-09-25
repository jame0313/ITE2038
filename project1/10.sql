SELECT Pokemon.name
FROM Pokemon
LEFT OUTER JOIN CatchedPokemon ON Pokemon.id = CatchedPokemon.pid
WHERE CatchedPokemon.owner_id IS NULL
ORDER BY Pokemon.name;
