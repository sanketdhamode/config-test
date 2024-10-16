public class TableConfig {
    private String name;
    private String sqlFile;
    private boolean useEventDate;
    private String sqlFilePath;

    // Getters and Setters

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSqlFile() {
        return sqlFile;
    }

    public void setSqlFile(String sqlFile) {
        this.sqlFile = sqlFile;
    }

    public boolean isUseEventDate() {
        return useEventDate;
    }

    public void setUseEventDate(boolean useEventDate) {
        this.useEventDate = useEventDate;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    public void setSqlFilePath(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
    }
}
