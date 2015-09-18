namespace java com.memsql.spark.examples.thrift

struct SubClass {
  1: string string_value
}

enum TestEnum {
  FIRST_VALUE = 1,
  SECOND_VALUE = 2
}

struct TestClass {
  1: bool bool_value,
  2: byte byte_value,
  3: i16 i16_value,
  4: i32 i32_value,
  5: i64 i64_value,
  6: double double_value,
  7: string string_value,
  8: binary binary_value,
  9: map<string, string> map_value,
  10: list<string> list_value,
  11: set<string> set_value,
  12: TestEnum enum_value,
  13: SubClass sub_class_value
}
