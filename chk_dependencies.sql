SELECT *
from user_dependencies
where referenced_name = 'table_name or what you need to search';


select distinct *
from all_dependencies a
start with a.referenced_name = 'table_name or what you need to search'
connect by NOCYCLE prior a.name = a.referenced_name;