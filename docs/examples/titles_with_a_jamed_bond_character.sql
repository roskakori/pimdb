-- Movies with a character named "James Bond" and the respective actor
select
    title.primary_title as "Title",
    title.start_year as "Year",
    name.primary_name as "Actor",
    "character".name as "Character"
from
    "character"
    join participation_to_character on
        participation_to_character.character_id = "character".id
    join participation on
        participation.id = participation_to_character.participation_id
    join name on
        name.id = participation.name_id
    join title on
        title.id = participation.title_id
    join title_type on
        title_type.id = title.title_type_id
where
    "character".name = 'James Bond'
    and title_type.name = 'movie'
order by
    title.start_year,
    name.primary_name,
    title.primary_title
