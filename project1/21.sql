;
SELECT DISTINCT Trainer.name
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id, Trainer.name, CatchedPokemon.pid
HAVING COUNT(*) >= 2
ORDER BY Trainer.name
