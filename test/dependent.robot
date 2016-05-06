
| *Setting* | *Value*  |           |              |
| Library   | RoboTest | ${MASTER} | ${DBCONNECT} |

| *Test case* | *Action*              | *Infile*    | *File*  | *Field* | *Depends on* |
| First test  | Grep file and compare | /etc/passwd | passwd1 | root    |
| Second test | Grep file and compare | /etc/passwd | passwd2 | bin     | First test   |
