-- Titles directed by Alan Smithee (using dataset tables)
select
    TitleBasics.primaryTitle,
    TitleBasics.startYear
from
    TitleBasics
    join TitlePrincipals on
        TitlePrincipals.tconst = TitleBasics.tconst
    join NameBasics on
        NameBasics.nconst = TitlePrincipals.nconst
where
    NameBasics.primaryName = 'Alan Smithee'
    and TitlePrincipals.category = 'director'
