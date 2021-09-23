SELECT Trainer.name, CatchedPokemon.nickname
FROM
(SELECT CatchedPokemon.owner_id AS id, MAX(CatchedPokemon.level) AS maxlevel
FROM CatchedPokemon
GROUP BY CatchedPokemon.owner_id
HAVING COUNT(*) >= 4) AS FourTrainer
JOIN Trainer ON FourTrainer.id = Trainer.id
JOIN CatchedPokemon ON FourTrainer.id = CatchedPokemon.owner_id AND CatchedPokemon.level = FourTrainer.maxlevel
ORDER BY CatchedPokemon.nickname;
