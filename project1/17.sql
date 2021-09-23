SELECT AVG(CatchedPokemon.level)
FROM CatchedPokemon
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id
WHERE Pokemon.type = 'water';
