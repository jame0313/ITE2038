;
SELECT Trainer.name
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name
HAVING COUNT(*)>=3
ORDER BY COUNT(*)
