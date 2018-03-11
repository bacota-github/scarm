create table teachers(
       id int primary key,
       name varchar(100)
);

insert into teachers (id, name) values (1, 'Fred Flintstone');
insert into teachers (id, name) values (2, 'Mister Magoo');

create table courses(
       id int primary key,
       subject varchar(100),
       prerequisite int references courses
);

insert into courses (id, subject, prerequisite)
       values (1, 'English Lit', null);
insert into courses (id, subject, prerequisite)
       values (2, 'Precalculus', null);
insert into courses (id, subject, prerequisite)
       values (3, 'Calculus', 3);

create table sections(
       course_id int references courses,
       semester int,
       section_number int,
       instructor int references teachers,
       room varchar(100),
       meeting_time  time
);

insert into sections values (1, 1, 1, 1, '203A', make_time(9, 0, 0.0));
insert into sections values (1, 1, 2, 1, '203A', make_time(10, 5, 0.0));
insert into sections values (2, 1, 1, 2, '203B', make_time(9, 0, 0.0));
insert into sections values (2, 1, 2, 2, '203B', make_time(10, 5, 0.0));
insert into sections values (3, 1, 1, 2, '203B', make_time(11, 30, 0.0));

alter table sections add constraint sections_pk primary key (course_id, semester, section_number);

create table students(
       id int primary key,
       name varchar(100),
       level int
);

insert into students values (1, 'Frodo', 1);
insert into students values (2, 'Bilbo', 3);
insert into students values (3, 'Samwise', 1);
insert into students values (4, 'Merry', 1);
insert into students values (5, 'Pippin', 1);

create table enrollments(
       student_id int references students,
       course_id int,
       semester int,
       section_number int,
       grade char(1)
);

insert into enrollments values (1, 1, 1, 1, null);
insert into enrollments values (2, 1, 1, 1, null);
insert into enrollments values (3, 1, 1, 1, null);
insert into enrollments values (4, 2, 1, 1, 'A');
insert into enrollments values (3, 2, 1, 1, 'B');
insert into enrollments values (2, 2, 1, 1, 'C');


alter table enrollments add constraint enrollments_sections_fk
foreign key (course_id, semester, section_number) references sections;

alter table enrollments add constraint enrollments_pk
primary key (student_id, course_id, semester, section_number);
