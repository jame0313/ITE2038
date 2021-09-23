SELECT AVG(CatchedPokemon.level)
FROM CatchedPokemon
JOIN Trainer ON CatchedPokemon.owner_id = Trainer.id
WHERE Trainer.name = 'Red';
