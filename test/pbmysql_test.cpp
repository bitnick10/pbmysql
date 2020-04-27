#include "gtest/gtest.h"

#include "../pbmysql/pbmysql.h"
#include "person.pb.h"
#include "pwd.h"

using namespace ns1;

TEST(bpmysql, Base) {
    mysqlx::Session sess("192.168.0.172", 33060, "root", pwd());
    sess.sql("USE pbdb").execute();
    Person person;
    person.mutable_born()->set_seconds(time(0));
    pbmysql::CreateTableIfNotExists(sess, person, "id");

    person.set_name("Jonh1");
    Person person2;
    person2.set_id(2);
    person2.set_name("David");
    time_t p2born = time(0) + 1;
    person2.mutable_born()->set_seconds(p2born);

    pbmysql::ReplaceInto(sess, person);
    pbmysql::ReplaceInto(sess, person2);

    auto db = sess.getSchema("pbdb");
    auto table = db.getTable("person");
    std::vector<Person> rs = pbmysql::Select<Person>(table, "id>=0");

    EXPECT_TRUE(rs.size() >= 2);
    EXPECT_EQ(rs[0].id(), 0);
    EXPECT_EQ(rs[1].id(), 2);
    EXPECT_EQ(rs[1].name(), "David");
    EXPECT_EQ(rs[1].born().seconds(), p2born);
}
