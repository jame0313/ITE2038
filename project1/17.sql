SELECT AVG(CatchedPokemon.level)
FROM CatchedPokemon
JOIN Pokemon ON Pokemon.type = 'Water' AND CatchedPokemon.pid = Pokemon.id;
