;
SELECT Pokemon.name
FROM Pokemon
JOIN CatchedPokemon ON Pokemon.id = CatchedPokemon.pid
JOIN Trainer ON Trainer.hometown IN ('Sangnok City', 'Blue City') AND Trainer.id = CatchedPokemon.owner_id
GROUP BY Pokemon.id, Pokemon.name
HAVING COUNT(DISTINCT Trainer.hometown) >= 2
ORDER BY Pokemon.name
