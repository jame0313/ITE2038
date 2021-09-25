SELECT Second_Pokemon.name
FROM Pokemon AS Second_Pokemon
JOIN Evolution AS First_Evolution ON Second_Pokemon.id = First_Evolution.after_id
LEFT OUTER JOIN Evolution AS NULL_Evolution ON First_Evolution.before_id = NULL_Evolution.after_id
WHERE NULL_Evolution.after_id IS NULL
ORDER BY Second_Pokemon.name;