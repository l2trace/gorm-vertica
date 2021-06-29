
    create table test_tables (
        id  bigint  not null,
        name varchar(255),
        active bool default  true,
        created_at timestamp  ,
        modified_at timestamp ,
        point_1   float ,
        point_2   float ,
        binary_field_1 BINARY(15),
        binary_field_2 VARBINARY(24),
        char_field_1 char(8),
        char_field_2 VARCHAR(255)




    );
