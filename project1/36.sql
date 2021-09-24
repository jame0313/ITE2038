SELECT Pokemon.name
From Pokemon
JOIN Evolution AS First_Evolution ON First_Evolution.after_id = Pokemon.id
LEFT OUTER JOIN Evolution AS Second_Evolution ON Second_Evolution.before_id = Pokemon.id
WHERE Pokemon.type = 'Water' AND Second_Evolution.after_id IS NULL
ORDER BY Pokemon.name;
