SELECT SUM(CatchedPokemon.level)
FROM CatchedPokemon
JOIN Trainer ON Trainer.name = 'Matis' AND Trainer.id = CatchedPokemon.owner_id;
