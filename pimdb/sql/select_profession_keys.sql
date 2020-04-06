select distinct
    primaryProfession
from
    name_basics
where
    primaryProfession is not null
    and primaryProfession != ''
