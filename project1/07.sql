;
SELECT CatchedPokemon.nickname
FROM
(SELECT City.name AS name, MAX(CatchedPokemon.level) AS maxlevel
FROM CatchedPokemon
JOIN Trainer ON Trainer.id = CatchedPokemon.owner_id
JOIN City ON City.name = Trainer.hometown
GROUP BY City.name) AS CityMaxLevel
JOIN Trainer ON Trainer.hometown = CityMaxLevel.name
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id AND CatchedPokemon.level = CityMaxLevel.maxlevel
ORDER BY CatchedPokemon.nickname;
