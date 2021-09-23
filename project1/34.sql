SELECT Third_Pokemon.name
FROM Pokemon
JOIN Evolution AS First_Evolution ON Pokemon.name = 'Charmander' AND First_Evolution.before_id = Pokemon.id
JOIN Evolution AS Second_Evolution ON Second_Evolution.before_id = First_Evolution.after_id
JOIN Pokemon AS Third_Pokemon ON Second_Evolution.after_id = Third_Pokemon.id;
