SELECT City.name, CatchedPokemon.nickname
FROM CatchedPokemon
JOIN Trainer ON Trainer.id = CatchedPokemon.owner_id
JOIN City ON City.name = Trainer.hometown
WHERE CatchedPokemon.level >= ALL(
SELECT CP.level
FROM CatchedPokemon AS CP
JOIN Trainer AS T ON T.id = CP.owner_id
JOIN City AS C ON C.name = T.hometown
WHERE City.name = C.name 
)
ORDER BY City.name
