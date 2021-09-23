SELECT DISTINCT Trainer.name
From Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
JOIN Pokemon ON Pokemon.name = 'Pikachu' AND CatchedPokemon.pid = Pokemon.id
ORDER BY Trainer.name
