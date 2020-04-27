#pragma once

#include <mysqlx/xdevapi.h>
#include <fmt/format.h>
#include <google/protobuf/message.h>

#ifdef  GetMessage
#undef  GetMessage
#endif

namespace pbmysql {

inline std::string GetMySQLTypeName(const std::string& messageTypeName) {
    if (messageTypeName == "string")
        return "TEXT";
    if (messageTypeName == "int32")
        return "INTEGER";
    if (messageTypeName == "int64")
        return "INTEGER";
    if (messageTypeName == "uint32")
        return "INTEGER";
    if (messageTypeName == "uint64")
        return "INTEGER";
    if (messageTypeName == "float")
        return "DOUBLE";
    if (messageTypeName == "double")
        return "DOUBLE";
    assert(false);
    return "TEXT";
}
inline std::string ToInsertString(const google::protobuf::Message& m, const std::string& insertType = "INSERT INTO") {
    using namespace google::protobuf;
    const Descriptor* desc = m.GetDescriptor();
    const Reflection* refl = m.GetReflection();
    std::string tableName = desc->name();
    std::string query = fmt::format("{0} {1} VALUES(", insertType, tableName);
    int fieldCount = desc->field_count();

    for (int i = 0; i < fieldCount; i++) {
        const FieldDescriptor* field = desc->field(i);
        switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
            query += fmt::format("'{0}',", refl->GetString(m, field));
            break;
        case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
            query += fmt::format("{0},", refl->GetInt32(m, field));
            break;
        case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
            query += fmt::format("{0},", refl->GetInt64(m, field));
            break;
        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
            query += fmt::format("{0},", refl->GetFloat(m, field));
            break;
        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
            query += fmt::format("{0},", refl->GetDouble(m, field));
            break;
        case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
            const google::protobuf::Message& subm = refl->GetMessage(m, field);
            const Descriptor* subdesc = subm.GetDescriptor();
            const Reflection* subrefl = subm.GetReflection();
            const FieldDescriptor* subfield = subdesc->field(0);
            if (subdesc->name() == "Timestamp") {
                query += fmt::format("FROM_UNIXTIME({0}),", subrefl->GetInt64(subm, subfield));
            }
            break;
        }
        default:
            assert(false);
            break;
        }
    }
    query.pop_back();
    query += ");";
    return query;
}
inline std::string ToCreateTableQuery(const google::protobuf::Message& m, const std::string& primaryKey) {
    using namespace google::protobuf;
    const Descriptor* desc = m.GetDescriptor();
    const Reflection* refl = m.GetReflection();
    std::string tableName = desc->name();
    std::string query = fmt::format("CREATE TABLE if not exists {0} (", tableName);
    int fieldCount = desc->field_count();

    for (int i = 0; i < fieldCount; i++) {
        const FieldDescriptor* field = desc->field(i);
        if (field->type() == FieldDescriptor::TYPE_MESSAGE) {
            const google::protobuf::Message& sub = refl->GetMessage(m, field);
            const Descriptor* subdesc = sub.GetDescriptor();
            if (subdesc->name() == "Timestamp") {
                query += fmt::format("{0} {1},", field->name(), "TIMESTAMP");
            }
        } else {
            query += fmt::format("{0} {1},", field->name(), GetMySQLTypeName(field->type_name()));
        }
    }
    query += "PRIMARY KEY (" + primaryKey + "));";
    return query;
}
inline void CreateTableIfNotExists(mysqlx::Session& sess, const google::protobuf::Message& m, const std::string& primaryKey) {
    try {
        auto query = ToCreateTableQuery(m, primaryKey);
        sess.sql(query).execute();
    } catch (std::exception e) {
        std::cout << e.what();
    }
}
inline void ReplaceInto(mysqlx::Session& sess, const google::protobuf::Message& m) {
    try {
        auto query = ToInsertString(m, "REPLACE INTO");
        sess.sql(query).execute();
    } catch (std::exception e) {
        std::cout << e.what();
    }
}
inline std::vector<std::string> GetColumnNames(const google::protobuf::Message& m) {
    using namespace google::protobuf;
    std::vector<std::string> ret;
    const Descriptor* desc = m.GetDescriptor();
    const Reflection* refl = m.GetReflection();
    int fieldCount = desc->field_count();

    for (int i = 0; i < fieldCount; i++) {
        const FieldDescriptor* field = desc->field(i);
        if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            ret.push_back("UNIX_TIMESTAMP(" + field->name() + ")");
        } else {
            ret.push_back(field->name());
        }
    }
    return ret;
}
template<typename T>
std::vector<T> Select(mysqlx::Table& table, const std::string& condition = "") {
    using namespace google::protobuf;
    std::vector<T> ret;
    T m;
    const Descriptor* desc = m.GetDescriptor();
    const Reflection* refl = m.GetReflection();
    int fieldCount = desc->field_count();
    std::string tableName = desc->name();

    auto res = table.select(GetColumnNames(m)).where(condition).execute();
    std::list<mysqlx::Row> rows = res.fetchAll();

    for (auto & row : rows) {
        for (int i = 0; i < fieldCount; i++) {
            const FieldDescriptor* field = desc->field(i);
            switch (field->cpp_type()) {
            case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                refl->SetString(&m, field, row[i].get<std::string>());
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                refl->SetInt32(&m, field, row[i].get<std::int32_t>());
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                refl->SetInt64(&m, field, row[i].get<std::int64_t>());
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                refl->SetFloat(&m, field, row[i].get<float>());
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                refl->SetDouble(&m, field, row[i].get<double>());
                break;
            case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
                google::protobuf::Message& subm = const_cast<google::protobuf::Message&>(refl->GetMessage(m, field));
                const Descriptor* subdesc = subm.GetDescriptor();
                const Reflection* subrefl = subm.GetReflection();
                const FieldDescriptor* subfield = subdesc->field(0);
                if (subdesc->name() == "Timestamp") {
                    subrefl->SetInt64(&subm, subfield, row[i].get<std::int64_t>());
                }
                break;
            }
            default:
                assert(false);
                break;
            }
        }
        ret.push_back(m);
    }
    return ret;
}
}
