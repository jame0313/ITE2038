;
SELECT DISTINCT Trainer.name
From Pokemon
JOIN CatchedPokemon ON Pokemon.name LIKE 'P%' AND Pokemon.id = CatchedPokemon.pid
JOIN Trainer ON Trainer.hometown = 'Sangnok city' AND Trainer.id = CatchedPokemon.owner_id
ORDER BY Trainer.name
