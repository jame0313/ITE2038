;
SELECT Pokemon.name
From Pokemon
LEFT OUTER JOIN Evolution ON Evolution.before_id = Pokemon.id
WHERE Pokemon.type = 'Water' AND Evolution.after_id IS NULL
ORDER BY Pokemon.name
