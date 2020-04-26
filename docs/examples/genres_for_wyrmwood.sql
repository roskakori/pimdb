-- Genres for title "Wyrmwood: Road of the Dead"
select
	title.tconst,
	title.primary_title,
	genre.name as "genre.name"
from
	title
	join title_to_genre on
		title_to_genre.title_id = title.id
	join genre on
		genre.id = title_to_genre.genre_id
where
	title.tconst  = 'tt2535470'  -- "Wyrmwood: Road of the Dead"
order by
	title.tconst,
	title_to_genre.ordering
