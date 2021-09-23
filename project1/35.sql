;
SELECT Trainer.name, COUNT(*)
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name
ORDER BY Trainer.name
