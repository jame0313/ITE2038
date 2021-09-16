SELECT City.name, COUNT(*)
FROM City
JOIN Trainer ON City.name = Trainer.hometown
GROUP BY City.name
ORDER BY COUNT(*) DESC
LIMIT 1
