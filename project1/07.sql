SELECT CatchedPokemon.nickname
FROM
(SELECT Trainer.hometown AS name, MAX(CatchedPokemon.level) AS maxlevel
FROM CatchedPokemon
JOIN Trainer ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.hometown) AS CityMaxLevel
JOIN Trainer ON Trainer.hometown = CityMaxLevel.name
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id AND CatchedPokemon.level = CityMaxLevel.maxlevel
ORDER BY CatchedPokemon.nickname;
