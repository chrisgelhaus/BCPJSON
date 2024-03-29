DECLARE @Schema VARCHAR(MAX) = '';
                                   DECLARE @TableName VARCHAR(MAX) = '';
                                   DECLARE @Columns VARCHAR(MAX) = '';
                                   DECLARE @SortOrder VARCHAR(MAX) = '';

                                   SELECT   @Columns = COALESCE(@Columns + ', ', '') + CASE sc.system_type_id
                                                                                       WHEN 240
                                                                                       THEN sc.name + '.STAsText() AS ' + sc.name
                                                                                       ELSE sc.name
                                                                                     END
                                   FROM     sys.tables st
                                            INNER JOIN sys.columns sc ON sc.object_id = st.object_id
                                   WHERE    st.object_id = {0}
                                   ORDER BY sc.column_id;

                                   SELECT   @Schema = ss.name 
                                   FROM     sys.tables st
                                            INNER JOIN sys.schemas ss ON ss.schema_id = st.schema_id
                                   WHERE    st.object_id = {0};

                                   SELECT   @TableName = st.name 
                                   FROM     sys.tables st
                                   WHERE    st.object_id = {0};

									SELECT   @SortOrder = COALESCE(@SortOrder + ', ', '') + sicn.name + ' '
											+ CASE sic.is_descending_key
												WHEN 0 THEN 'ASC'
												ELSE 'DESC'
												END
									FROM     sys.tables st
											INNER JOIN sys.indexes si ON si.object_id = st.object_id
											INNER JOIN sys.index_columns sic ON sic.object_id = si.object_id
																				AND sic.index_id = si.index_id
											INNER JOIN sys.columns sicn ON sicn.object_id = st.object_id
																			AND sicn.column_id = sic.column_id
											INNER JOIN (SELECT TOP 1 
												sys.indexes.name
											FROM     sys.tables
													INNER JOIN sys.indexes ON sys.indexes.object_id = sys.tables.object_id
													INNER JOIN sys.index_columns ON sys.index_columns.object_id = sys.indexes.object_id
																						AND sys.index_columns.index_id = sys.indexes.index_id
													INNER JOIN sys.columns ON sys.columns.object_id = sys.tables.object_id
																					AND sys.columns.column_id = sys.index_columns.column_id
											WHERE   sys.index_columns.is_included_column = 0 
													AND sys.tables.object_id = {0}
											ORDER BY sys.tables.object_id, sys.indexes.is_primary_key DESC, sys.indexes.is_unique DESC, sys.index_columns.key_ordinal ASC) as idxname ON idxname.name = si.name
									WHERE    st.object_id = {0}
											AND sic.is_included_column = 0 
									ORDER BY sic.key_ordinal ASC;

                                   SELECT @Schema, @TableName, @Columns, @SortOrder;