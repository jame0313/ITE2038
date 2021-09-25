SELECT AVG(CatchedPokemon.level)
FROM CatchedPokemon
JOIN Trainer ON Trainer.name = 'Red' AND CatchedPokemon.owner_id = Trainer.id;