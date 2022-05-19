package TestReports;

public enum TestType {
    INSERT("InsertTest", TestCategory.CRUD),
    BATCH_INSERT("BatchInsertTest", TestCategory.CRUD),
    UPDATE("UpdateTest", TestCategory.CRUD),
    DELETE("DeleteTest", TestCategory.CRUD),

    SELECT_BY_INT("SelectByIntegerTest", TestCategory.QUERY),
    SELECT_EDGE_WITH_VERTEX_PARAMS("SelectEdgesWithVertexParametersTest", TestCategory.QUERY),
    SELECT_BY_STRING_WITH_LIKE("SelectByStringWithLikeTest", TestCategory.QUERY),
    SELECT_MULTIPLE_PARAMS("SelectByMultipleParametersTest", TestCategory.QUERY),

    COUNT_NEIGHBOURS("CountNeighboursTest", TestCategory.QUERY),
    GROUP_BY("GroupByTest", TestCategory.QUERY),

    GET_NEIGHBOURS("GetNeighboursTest", TestCategory.QUERY),
    GET_VERTICES_WITHOUT_EDGES("GetVerticesWithoutEdgesTest", TestCategory.QUERY),
    GET_COMMON_NEIGHBOURS("GetCommonNeighboursTest", TestCategory.QUERY);

    private final String text;
    private final TestCategory category;


    TestType(final String text, final TestCategory category) {
        this.text = text;
        this.category = category;
    }

    @Override
    public String toString() {
        return text;
    }

    public boolean isQueryTest() {
        return this.category == TestCategory.QUERY;
    }

    private enum TestCategory {
        CRUD,
        QUERY
    }
}
