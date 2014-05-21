/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

grammar Cql;

options {
    language = Java;
}

@header {
    package org.apache.cassandra.cql3;

    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.EnumSet;
    import java.util.HashMap;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;

    import org.apache.cassandra.auth.Permission;
    import org.apache.cassandra.auth.DataResource;
    import org.apache.cassandra.auth.IResource;
    import org.apache.cassandra.cql3.*;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.cql3.functions.FunctionCall;
    import org.apache.cassandra.db.marshal.CollectionType;
    import org.apache.cassandra.exceptions.ConfigurationException;
    import org.apache.cassandra.exceptions.InvalidRequestException;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.utils.Pair;
}

@members {
    private final List<String> recognitionErrors = new ArrayList<String>();
    private final List<ColumnIdentifier> bindVariables = new ArrayList<ColumnIdentifier>();

    public AbstractMarker.Raw newBindVariables(ColumnIdentifier name)
    {
        AbstractMarker.Raw marker = new AbstractMarker.Raw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public AbstractMarker.INRaw newINBindVariables(ColumnIdentifier name)
    {
        AbstractMarker.INRaw marker = new AbstractMarker.INRaw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        recognitionErrors.add(hdr + " " + msg);
    }

    public void addRecognitionError(String msg)
    {
        recognitionErrors.add(msg);
    }

    public List<String> getRecognitionErrors()
    {
        return recognitionErrors;
    }

    public void throwLastRecognitionError() throws SyntaxException
    {
        if (recognitionErrors.size() > 0)
            throw new SyntaxException(recognitionErrors.get((recognitionErrors.size()-1)));
    }

    public Map<String, String> convertPropertyMap(Maps.Literal map)
    {
        if (map == null || map.entries == null || map.entries.isEmpty())
            return Collections.<String, String>emptyMap();

        Map<String, String> res = new HashMap<String, String>(map.entries.size());

        for (Pair<Term.Raw, Term.Raw> entry : map.entries)
        {
            // Because the parser tries to be smart and recover on error (to
            // allow displaying more than one error I suppose), we have null
            // entries in there. Just skip those, a proper error will be thrown in the end.
            if (entry.left == null || entry.right == null)
                break;

            if (!(entry.left instanceof Constants.Literal))
            {
                String msg = "Invalid property name: " + entry.left;
                if (entry.left instanceof AbstractMarker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }
            if (!(entry.right instanceof Constants.Literal))
            {
                String msg = "Invalid property value: " + entry.right + " for property: " + entry.left;
                if (entry.right instanceof AbstractMarker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }

            res.put(((Constants.Literal)entry.left).getRawText(), ((Constants.Literal)entry.right).getRawText());
        }

        return res;
    }

    public void addRawUpdate(List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key, Operation.RawUpdate update)
    {
        for (Pair<ColumnIdentifier, Operation.RawUpdate> p : operations)
        {
            if (p.left.equals(key) && !p.right.isCompatibleWith(update))
                addRecognitionError("Multiple incompatible setting of column " + key);
        }
        operations.add(Pair.create(key, update));
    }
}

@lexer::header {
    package org.apache.cassandra.cql3;

    import org.apache.cassandra.exceptions.SyntaxException;
}

@lexer::members {
    List<Token> tokens = new ArrayList<Token>();

    public void emit(Token token)
    {
        state.token = token;
        tokens.add(token);
    }

    public Token nextToken()
    {
        super.nextToken();
        if (tokens.size() == 0)
            return Token.EOF_TOKEN;
        return tokens.remove(0);
    }

    private List<String> recognitionErrors = new ArrayList<String>();

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        recognitionErrors.add(hdr + " " + msg);
    }

    public List<String> getRecognitionErrors()
    {
        return recognitionErrors;
    }

    public void throwLastRecognitionError() throws SyntaxException
    {
        if (recognitionErrors.size() > 0)
            throw new SyntaxException(recognitionErrors.get((recognitionErrors.size()-1)));
    }
}

/** STATEMENTS **/

query returns [ParsedStatement stmnt]
    : st=cqlStatement (';')* EOF { $stmnt = st; }
    ;

cqlStatement returns [ParsedStatement stmt]
    @after{ if (stmt != null) stmt.setBoundVariables(bindVariables); }
    : st1= selectStatement             { $stmt = st1; }
    | st2= insertStatement             { $stmt = st2; }
    | st3= updateStatement             { $stmt = st3; }
    | st4= batchStatement              { $stmt = st4; }
    | st5= deleteStatement             { $stmt = st5; }
    | st6= useStatement                { $stmt = st6; }
    | st7= truncateStatement           { $stmt = st7; }
    | st8= createKeyspaceStatement     { $stmt = st8; }
    | st9= createTableStatement        { $stmt = st9; }
    | st10=createIndexStatement        { $stmt = st10; }
    | st11=dropKeyspaceStatement       { $stmt = st11; }
    | st12=dropTableStatement          { $stmt = st12; }
    | st13=dropIndexStatement          { $stmt = st13; }
    | st14=alterTableStatement         { $stmt = st14; }
    | st15=alterKeyspaceStatement      { $stmt = st15; }
    | st16=grantStatement              { $stmt = st16; }
    | st17=revokeStatement             { $stmt = st17; }
    | st18=listPermissionsStatement    { $stmt = st18; }
    | st19=createUserStatement         { $stmt = st19; }
    | st20=alterUserStatement          { $stmt = st20; }
    | st21=dropUserStatement           { $stmt = st21; }
    | st22=listUsersStatement          { $stmt = st22; }
    | st23=createTriggerStatement      { $stmt = st23; }
    | st24=dropTriggerStatement        { $stmt = st24; }
    ;

/*
 * USE <KEYSPACE>;
 */
useStatement returns [UseStatement stmt]
    : K_USE ks=keyspaceName { $stmt = new UseStatement(ks); }
    ;

/**
 * SELECT <expression>
 * FROM <CF>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement returns [SelectStatement.RawStatement expr]
    @init {
        boolean isDistinct = false;
        boolean isCount = false;
        ColumnIdentifier countAlias = null;
        Term.Raw limit = null;
        Map<ColumnIdentifier, Boolean> orderings = new LinkedHashMap<ColumnIdentifier, Boolean>();
        boolean allowFiltering = false;
    }
    : K_SELECT ( ( K_DISTINCT { isDistinct = true; } )? sclause=selectClause
               | (K_COUNT '(' sclause=selectCountClause ')' { isCount = true; } (K_AS c=cident { countAlias = c; })?) )
      K_FROM cf=columnFamilyName
      ( K_WHERE wclause=whereClause )?
      ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
      ( K_LIMIT rows=intValue { limit = rows; } )?
      ( K_ALLOW K_FILTERING  { allowFiltering = true; } )?
      {
          SelectStatement.Parameters params = new SelectStatement.Parameters(orderings,
                                                                             isDistinct,
                                                                             isCount,
                                                                             countAlias,
                                                                             allowFiltering);
          $expr = new SelectStatement.RawStatement(cf, params, sclause, wclause, limit);
      }
    ;

selectClause returns [List<RawSelector> expr]
    : t1=selector { $expr = new ArrayList<RawSelector>(); $expr.add(t1); } (',' tN=selector { $expr.add(tN); })*
    | '\*' { $expr = Collections.<RawSelector>emptyList();}
    ;

selector returns [RawSelector s]
    @init{ ColumnIdentifier alias = null; }
    : us=unaliasedSelector (K_AS c=cident { alias = c; })? { $s = new RawSelector(us, alias); }
    ;

unaliasedSelector returns [Selectable s]
    : c=cident                                  { $s = c; }
    | K_WRITETIME '(' c=cident ')'              { $s = new Selectable.WritetimeOrTTL(c, true); }
    | K_TTL       '(' c=cident ')'              { $s = new Selectable.WritetimeOrTTL(c, false); }
    | f=functionName args=selectionFunctionArgs { $s = new Selectable.WithFunction(f, args); }
    ;

selectionFunctionArgs returns [List<Selectable> a]
    : '(' ')' { $a = Collections.emptyList(); }
    | '(' s1=unaliasedSelector { List<Selectable> args = new ArrayList<Selectable>(); args.add(s1); }
          ( ',' sn=unaliasedSelector { args.add(sn); } )*
       ')' { $a = args; }
    ;

selectCountClause returns [List<RawSelector> expr]
    : '\*'           { $expr = Collections.<RawSelector>emptyList();}
    | i=INTEGER      { if (!i.getText().equals("1")) addRecognitionError("Only COUNT(1) is supported, got COUNT(" + i.getText() + ")"); $expr = Collections.<RawSelector>emptyList();}
    ;

whereClause returns [List<Relation> clause]
    @init{ $clause = new ArrayList<Relation>(); }
    : relation[$clause] (K_AND relation[$clause])*
    ;

orderByClause[Map<ColumnIdentifier, Boolean> orderings]
    @init{
        ColumnIdentifier orderBy = null;
        boolean reversed = false;
    }
    : c=cident { orderBy = c; } (K_ASC | K_DESC { reversed = true; })? { orderings.put(c, reversed); }
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING TIMESTAMP <long>;
 *
 */
insertStatement returns [UpdateStatement.ParsedInsert expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<ColumnIdentifier> columnNames  = new ArrayList<ColumnIdentifier>();
        List<Term.Raw> values = new ArrayList<Term.Raw>();
        boolean ifNotExists = false;
    }
    : K_INSERT K_INTO cf=columnFamilyName
          '(' c1=cident { columnNames.add(c1); }  ( ',' cn=cident { columnNames.add(cn); } )* ')'
        K_VALUES
          '(' v1=term { values.add(v1); } ( ',' vn=term { values.add(vn); } )* ')'

        ( K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
        ( usingClause[attrs] )?
      {
          $expr = new UpdateStatement.ParsedInsert(cf,
                                                   attrs,
                                                   columnNames,
                                                   values,
                                                   ifNotExists);
      }
    ;

usingClause[Attributes.Raw attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
    ;

usingClauseObjective[Attributes.Raw attrs]
    : K_TIMESTAMP ts=intValue { attrs.timestamp = ts; }
    | K_TTL t=intValue { attrs.timeToLive = t; }
    ;

/**
 * UPDATE <CF>
 * USING TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 */
updateStatement returns [UpdateStatement.ParsedUpdate expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations = new ArrayList<Pair<ColumnIdentifier, Operation.RawUpdate>>();
    }
    : K_UPDATE cf=columnFamilyName
      ( usingClause[attrs] )?
      K_SET columnOperation[operations] (',' columnOperation[operations])*
      K_WHERE wclause=whereClause
      ( K_IF conditions=updateConditions )?
      {
          return new UpdateStatement.ParsedUpdate(cf,
                                                  attrs,
                                                  operations,
                                                  wclause,
                                                  conditions == null ? Collections.<Pair<ColumnIdentifier, ColumnCondition.Raw>>emptyList() : conditions);
     }
    ;

updateConditions returns [List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions]
    @init { conditions = new ArrayList<Pair<ColumnIdentifier, ColumnCondition.Raw>>(); }
    : columnCondition[conditions] ( K_AND columnCondition[conditions] )*
    ;


/**
 * DELETE name1, name2
 * FROM <CF>
 * USING TIMESTAMP <long>
 * WHERE KEY = keyname
   [IF (EXISTS | name = value, ...)];
 */
deleteStatement returns [DeleteStatement.Parsed expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<Operation.RawDeletion> columnDeletions = Collections.emptyList();
        boolean ifExists = false;
    }
    : K_DELETE ( dels=deleteSelection { columnDeletions = dels; } )?
      K_FROM cf=columnFamilyName
      ( usingClauseDelete[attrs] )?
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { ifExists = true; } | conditions=updateConditions ))?
      {
          return new DeleteStatement.Parsed(cf,
                                            attrs,
                                            columnDeletions,
                                            wclause,
                                            conditions == null ? Collections.<Pair<ColumnIdentifier, ColumnCondition.Raw>>emptyList() : conditions,
                                            ifExists);
      }
    ;

deleteSelection returns [List<Operation.RawDeletion> operations]
    : { $operations = new ArrayList<Operation.RawDeletion>(); }
          t1=deleteOp { $operations.add(t1); }
          (',' tN=deleteOp { $operations.add(tN); })*
    ;

deleteOp returns [Operation.RawDeletion op]
    : c=cident                { $op = new Operation.ColumnDeletion(c); }
    | c=cident '[' t=term ']' { $op = new Operation.ElementDeletion(c, t); }
    ;

usingClauseDelete[Attributes.Raw attrs]
    : K_USING K_TIMESTAMP ts=intValue { attrs.timestamp = ts; }
    ;

/**
 * BEGIN BATCH
 *   UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 *   UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 *   UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   DELETE name1, name2 FROM <CF> WHERE key = <key>
 *   DELETE name3, name4 FROM <CF> WHERE key = <key>
 *   ...
 * APPLY BATCH
 */
batchStatement returns [BatchStatement.Parsed expr]
    @init {
        BatchStatement.Type type = BatchStatement.Type.LOGGED;
        List<ModificationStatement.Parsed> statements = new ArrayList<ModificationStatement.Parsed>();
        Attributes.Raw attrs = new Attributes.Raw();
    }
    : K_BEGIN
      ( K_UNLOGGED { type = BatchStatement.Type.UNLOGGED; } | K_COUNTER { type = BatchStatement.Type.COUNTER; } )?
      K_BATCH ( usingClause[attrs] )?
          ( s=batchStatementObjective ';'? { statements.add(s); } )*
      K_APPLY K_BATCH
      {
          return new BatchStatement.Parsed(type, attrs, statements);
      }
    ;

batchStatementObjective returns [ModificationStatement.Parsed statement]
    : i=insertStatement  { $statement = i; }
    | u=updateStatement  { $statement = u; }
    | d=deleteStatement  { $statement = d; }
    ;

/**
 * CREATE KEYSPACE [IF NOT EXISTS] <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement returns [CreateKeyspaceStatement expr]
    @init {
        KSPropDefs attrs = new KSPropDefs();
        boolean ifNotExists = false;
    }
    : K_CREATE K_KEYSPACE (K_IF K_NOT K_EXISTS { ifNotExists = true; } )? ks=keyspaceName
      K_WITH properties[attrs] { $expr = new CreateKeyspaceStatement(ks, attrs, ifNotExists); }
    ;

/**
 * CREATE COLUMNFAMILY [IF NOT EXISTS] <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createTableStatement returns [CreateTableStatement.RawStatement expr]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      cf=columnFamilyName { $expr = new CreateTableStatement.RawStatement(cf, ifNotExists); }
      cfamDefinition[expr]
    ;

cfamDefinition[CreateTableStatement.RawStatement expr]
    : '(' cfamColumns[expr] ( ',' cfamColumns[expr]? )* ')'
      ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )*)?
    ;

cfamColumns[CreateTableStatement.RawStatement expr]
    : k=cident v=comparatorType { boolean isStatic=false; } (K_STATIC {isStatic = true;})? { $expr.addDefinition(k, v, isStatic); }
        (K_PRIMARY K_KEY { $expr.addKeyAliases(Collections.singletonList(k)); })?
    | K_PRIMARY K_KEY '(' pkDef[expr] (',' c=cident { $expr.addColumnAlias(c); } )* ')'
    ;

pkDef[CreateTableStatement.RawStatement expr]
    : k=cident { $expr.addKeyAliases(Collections.singletonList(k)); }
    | '(' { List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); } k1=cident { l.add(k1); } ( ',' kn=cident { l.add(kn); } )* ')' { $expr.addKeyAliases(l); }
    ;

cfamProperty[CreateTableStatement.RawStatement expr]
    : property[expr.properties]
    | K_COMPACT K_STORAGE { $expr.setCompactStorage(); }
    | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] (',' cfamOrdering[expr])* ')'
    ;

cfamOrdering[CreateTableStatement.RawStatement expr]
    @init{ boolean reversed=false; }
    : k=cident (K_ASC | K_DESC { reversed=true;} ) { $expr.setOrdering(k, reversed); }
    ;

/**
 * CREATE INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>);
 * CREATE CUSTOM INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>) USING <indexClass>;
 */
createIndexStatement returns [CreateIndexStatement expr]
    @init {
        IndexPropDefs props = new IndexPropDefs();
        boolean ifNotExists = false;
    }
    : K_CREATE (K_CUSTOM { props.isCustom = true; })? K_INDEX (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
        (idxName=IDENT)? K_ON cf=columnFamilyName '(' id=cident ')'
        (K_USING cls=STRING_LITERAL { props.customClass = $cls.text; })?
        (K_WITH properties[props])?
      { $expr = new CreateIndexStatement(cf, $idxName.text, id, props, ifNotExists); }
    ;

/**
 * CREATE TRIGGER triggerName ON columnFamily USING 'triggerClass';
 */
createTriggerStatement returns [CreateTriggerStatement expr]
    : K_CREATE K_TRIGGER (name=IDENT) K_ON cf=columnFamilyName K_USING cls=STRING_LITERAL
      { $expr = new CreateTriggerStatement(cf, $name.text, $cls.text); }
    ;

/**
 * DROP TRIGGER triggerName ON columnFamily;
 */
dropTriggerStatement returns [DropTriggerStatement expr]
    : K_DROP K_TRIGGER (name=IDENT) K_ON cf=columnFamilyName
      { $expr = new DropTriggerStatement(cf, $name.text); }
    ;

/**
 * ALTER KEYSPACE <KS> WITH <property> = <value>;
 */
alterKeyspaceStatement returns [AlterKeyspaceStatement expr]
    @init { KSPropDefs attrs = new KSPropDefs(); }
    : K_ALTER K_KEYSPACE ks=keyspaceName
        K_WITH properties[attrs] { $expr = new AlterKeyspaceStatement(ks, attrs); }
    ;


/**
 * ALTER COLUMN FAMILY <CF> ALTER <column> TYPE <newtype>;
 * ALTER COLUMN FAMILY <CF> ADD <column> <newtype>;
 * ALTER COLUMN FAMILY <CF> DROP <column>;
 * ALTER COLUMN FAMILY <CF> WITH <property> = <value>;
 * ALTER COLUMN FAMILY <CF> RENAME <column> TO <column>;
 */
alterTableStatement returns [AlterTableStatement expr]
    @init {
        AlterTableStatement.Type type = null;
        CFPropDefs props = new CFPropDefs();
        Map<ColumnIdentifier, ColumnIdentifier> renames = new HashMap<ColumnIdentifier, ColumnIdentifier>();
        boolean isStatic = false;
    }
    : K_ALTER K_COLUMNFAMILY cf=columnFamilyName
          ( K_ALTER id=cident K_TYPE v=comparatorType { type = AlterTableStatement.Type.ALTER; }
          | K_ADD   id=cident v=comparatorType ({ isStatic=true; } K_STATIC)? { type = AlterTableStatement.Type.ADD; }
          | K_DROP  id=cident                         { type = AlterTableStatement.Type.DROP; }
          | K_WITH  properties[props]                 { type = AlterTableStatement.Type.OPTS; }
          | K_RENAME                                  { type = AlterTableStatement.Type.RENAME; }
               id1=cident K_TO toId1=cident { renames.put(id1, toId1); }
               ( K_AND idn=cident K_TO toIdn=cident { renames.put(idn, toIdn); } )*
          )
    {
        $expr = new AlterTableStatement(cf, type, id, v, props, renames, isStatic);
    }
    ;

/**
 * DROP KEYSPACE [IF EXISTS] <KSP>;
 */
dropKeyspaceStatement returns [DropKeyspaceStatement ksp]
    @init { boolean ifExists = false; }
    : K_DROP K_KEYSPACE (K_IF K_EXISTS { ifExists = true; } )? ks=keyspaceName { $ksp = new DropKeyspaceStatement(ks, ifExists); }
    ;

/**
 * DROP COLUMNFAMILY [IF EXISTS] <CF>;
 */
dropTableStatement returns [DropTableStatement stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_COLUMNFAMILY (K_IF K_EXISTS { ifExists = true; } )? cf=columnFamilyName { $stmt = new DropTableStatement(cf, ifExists); }
    ;

/**
 * DROP INDEX [IF EXISTS] <INDEX_NAME>
 */
dropIndexStatement returns [DropIndexStatement expr]
    @init { boolean ifExists = false; }
    : K_DROP K_INDEX (K_IF K_EXISTS { ifExists = true; } )? index=IDENT
      { $expr = new DropIndexStatement($index.text, ifExists); }
    ;

/**
  * TRUNCATE <CF>;
  */
truncateStatement returns [TruncateStatement stmt]
    : K_TRUNCATE cf=columnFamilyName { $stmt = new TruncateStatement(cf); }
    ;

/**
 * GRANT <permission> ON <resource> TO <username>
 */
grantStatement returns [GrantStatement stmt]
    : K_GRANT
          permissionOrAll
      K_ON
          resource
      K_TO
          username
      { $stmt = new GrantStatement($permissionOrAll.perms, $resource.res, $username.text); }
    ;

/**
 * REVOKE <permission> ON <resource> FROM <username>
 */
revokeStatement returns [RevokeStatement stmt]
    : K_REVOKE
          permissionOrAll
      K_ON
          resource
      K_FROM
          username
      { $stmt = new RevokeStatement($permissionOrAll.perms, $resource.res, $username.text); }
    ;

listPermissionsStatement returns [ListPermissionsStatement stmt]
    @init {
        IResource resource = null;
        String username = null;
        boolean recursive = true;
    }
    : K_LIST
          permissionOrAll
      ( K_ON resource { resource = $resource.res; } )?
      ( K_OF username { username = $username.text; } )?
      ( K_NORECURSIVE { recursive = false; } )?
      { $stmt = new ListPermissionsStatement($permissionOrAll.perms, resource, username, recursive); }
    ;

permission returns [Permission perm]
    : p=(K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE)
    { $perm = Permission.valueOf($p.text.toUpperCase()); }
    ;

permissionOrAll returns [Set<Permission> perms]
    : K_ALL ( K_PERMISSIONS )?       { $perms = Permission.ALL_DATA; }
    | p=permission ( K_PERMISSION )? { $perms = EnumSet.of($p.perm); }
    ;

resource returns [IResource res]
    : r=dataResource { $res = $r.res; }
    ;

dataResource returns [DataResource res]
    : K_ALL K_KEYSPACES { $res = DataResource.root(); }
    | K_KEYSPACE ks = keyspaceName { $res = DataResource.keyspace($ks.id); }
    | ( K_COLUMNFAMILY )? cf = columnFamilyName
      { $res = DataResource.columnFamily($cf.name.getKeyspace(), $cf.name.getColumnFamily()); }
    ;

/**
 * CREATE USER <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
createUserStatement returns [CreateUserStatement stmt]
    @init {
        UserOptions opts = new UserOptions();
        boolean superuser = false;
    }
    : K_CREATE K_USER username
      ( K_WITH userOptions[opts] )?
      ( K_SUPERUSER { superuser = true; } | K_NOSUPERUSER { superuser = false; } )?
      { $stmt = new CreateUserStatement($username.text, opts, superuser); }
    ;

/**
 * ALTER USER <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
alterUserStatement returns [AlterUserStatement stmt]
    @init {
        UserOptions opts = new UserOptions();
        Boolean superuser = null;
    }
    : K_ALTER K_USER username
      ( K_WITH userOptions[opts] )?
      ( K_SUPERUSER { superuser = true; } | K_NOSUPERUSER { superuser = false; } )?
      { $stmt = new AlterUserStatement($username.text, opts, superuser); }
    ;

/**
 * DROP USER <username>
 */
dropUserStatement returns [DropUserStatement stmt]
    : K_DROP K_USER username { $stmt = new DropUserStatement($username.text); }
    ;

/**
 * LIST USERS
 */
listUsersStatement returns [ListUsersStatement stmt]
    : K_LIST K_USERS { $stmt = new ListUsersStatement(); }
    ;

userOptions[UserOptions opts]
    : userOption[opts]
    ;

userOption[UserOptions opts]
    : k=K_PASSWORD v=STRING_LITERAL { opts.put($k.text, $v.text); }
    ;

/** DEFINITIONS **/

// Column Identifiers
cident returns [ColumnIdentifier id]
    : t=IDENT              { $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME        { $id = new ColumnIdentifier($t.text, true); }
    | k=unreserved_keyword { $id = new ColumnIdentifier(k, false); }
    ;

// Keyspace & Column family names
keyspaceName returns [String id]
    @init { CFName name = new CFName(); }
    : cfOrKsName[name, true] { $id = name.getKeyspace(); }
    ;

columnFamilyName returns [CFName name]
    @init { $name = new CFName(); }
    : (cfOrKsName[name, true] '.')? cfOrKsName[name, false]
    ;

cfOrKsName[CFName name, boolean isKs]
    : t=IDENT              { if (isKs) $name.setKeyspace($t.text, false); else $name.setColumnFamily($t.text, false); }
    | t=QUOTED_NAME        { if (isKs) $name.setKeyspace($t.text, true); else $name.setColumnFamily($t.text, true); }
    | k=unreserved_keyword { if (isKs) $name.setKeyspace(k, false); else $name.setColumnFamily(k, false); }
    ;

constant returns [Constants.Literal constant]
    : t=STRING_LITERAL { $constant = Constants.Literal.string($t.text); }
    | t=INTEGER        { $constant = Constants.Literal.integer($t.text); }
    | t=FLOAT          { $constant = Constants.Literal.floatingPoint($t.text); }
    | t=BOOLEAN        { $constant = Constants.Literal.bool($t.text); }
    | t=UUID           { $constant = Constants.Literal.uuid($t.text); }
    | t=HEXNUMBER      { $constant = Constants.Literal.hex($t.text); }
    | { String sign=""; } ('-' {sign = "-"; } )? t=(K_NAN | K_INFINITY) { $constant = Constants.Literal.floatingPoint(sign + $t.text); }
    ;

map_literal returns [Maps.Literal map]
    : '{' { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); }
          ( k1=term ':' v1=term { m.add(Pair.create(k1, v1)); } ( ',' kn=term ':' vn=term { m.add(Pair.create(kn, vn)); } )* )?
      '}' { $map = new Maps.Literal(m); }
    ;

set_or_map[Term.Raw t] returns [Term.Raw value]
    : ':' v=term { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); m.add(Pair.create(t, v)); }
          ( ',' kn=term ':' vn=term { m.add(Pair.create(kn, vn)); } )*
      { $value = new Maps.Literal(m); }
    | { List<Term.Raw> s = new ArrayList<Term.Raw>(); s.add(t); }
          ( ',' tn=term { s.add(tn); } )*
      { $value = new Sets.Literal(s); }
    ;

collection_literal returns [Term.Raw value]
    : '[' { List<Term.Raw> l = new ArrayList<Term.Raw>(); }
          ( t1=term { l.add(t1); } ( ',' tn=term { l.add(tn); } )* )?
      ']' { $value = new Lists.Literal(l); }
    | '{' t=term v=set_or_map[t] { $value = v; } '}'
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}' { $value = new Sets.Literal(Collections.<Term.Raw>emptyList()); }
    ;

value returns [Term.Raw value]
    : c=constant           { $value = c; }
    | l=collection_literal { $value = l; }
    | K_NULL               { $value = Constants.NULL_LITERAL; }
    | ':' id=cident        { $value = newBindVariables(id); }
    | QMARK                { $value = newBindVariables(null); }
    ;

intValue returns [Term.Raw value]
    :
    | t=INTEGER     { $value = Constants.Literal.integer($t.text); }
    | ':' id=cident { $value = newBindVariables(id); }
    | QMARK         { $value = newBindVariables(null); }
    ;

functionName returns [String s]
    : f=IDENT                       { $s = $f.text; }
    | u=unreserved_function_keyword { $s = u; }
    | K_TOKEN                       { $s = "token"; }
    ;

functionArgs returns [List<Term.Raw> a]
    : '(' ')' { $a = Collections.emptyList(); }
    | '(' t1=term { List<Term.Raw> args = new ArrayList<Term.Raw>(); args.add(t1); }
          ( ',' tn=term { args.add(tn); } )*
       ')' { $a = args; }
    ;

term returns [Term.Raw term]
    : v=value                          { $term = v; }
    | f=functionName args=functionArgs { $term = new FunctionCall.Raw(f, args); }
    | '(' c=comparatorType ')' t=term  { $term = new TypeCast(c, t); }
    ;

columnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations]
    : key=cident '=' t=term ('+' c=cident )?
      {
          if (c == null)
          {
              addRawUpdate(operations, key, new Operation.SetValue(t));
          }
          else
          {
              if (!key.equals(c))
                  addRecognitionError("Only expressions of the form X = <value> + X are supported.");
              addRawUpdate(operations, key, new Operation.Prepend(t));
          }
      }
    | key=cident '=' c=cident sig=('+' | '-') t=term
      {
          if (!key.equals(c))
              addRecognitionError("Only expressions of the form X = X " + $sig.text + "<value> are supported.");
          addRawUpdate(operations, key, $sig.text.equals("+") ? new Operation.Addition(t) : new Operation.Substraction(t));
      }
    | key=cident '=' c=cident i=INTEGER
      {
          // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
          if (!key.equals(c))
              // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
              addRecognitionError("Only expressions of the form X = X " + ($i.text.charAt(0) == '-' ? '-' : '+') + " <value> are supported.");
          addRawUpdate(operations, key, new Operation.Addition(Constants.Literal.integer($i.text)));
      }
    | key=cident '[' k=term ']' '=' t=term
      {
          addRawUpdate(operations, key, new Operation.SetElement(k, t));
      }
    ;

columnCondition[List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions]
    // Note: we'll reject duplicates later
    : key=cident '=' t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleEqual(t))); }
    | key=cident '[' element=term ']' '=' t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.collectionEqual(t, element))); } 
    ;

properties[PropertyDefinitions props]
    : property[props] (K_AND property[props])*
    ;

property[PropertyDefinitions props]
    : k=cident '=' (simple=propertyValue { try { $props.addProperty(k.toString(), simple); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } }
                   |   map=map_literal   { try { $props.addProperty(k.toString(), convertPropertyMap(map)); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } })
    ;

propertyValue returns [String str]
    : c=constant           { $str = c.getRawText(); }
    | u=unreserved_keyword { $str = u; }
    ;

relationType returns [Relation.Type op]
    : '='  { $op = Relation.Type.EQ; }
    | '<'  { $op = Relation.Type.LT; }
    | '<=' { $op = Relation.Type.LTE; }
    | '>'  { $op = Relation.Type.GT; }
    | '>=' { $op = Relation.Type.GTE; }
    ;

relation[List<Relation> clauses]
    : name=cident type=relationType t=term { $clauses.add(new Relation(name, type, t)); }
    | K_TOKEN 
        { List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); }
          '(' name1=cident { l.add(name1); } ( ',' namen=cident { l.add(namen); })* ')'
        type=relationType t=term
        {
            for (ColumnIdentifier id : l)
                $clauses.add(new Relation(id, type, t, true));
        }
    | name=cident K_IN { Term.Raw marker = null; } (QMARK { marker = newINBindVariables(null); } | ':' mid=cident { marker = newINBindVariables(mid); })
        { $clauses.add(new Relation(name, Relation.Type.IN, marker)); }
    | name=cident K_IN { Relation rel = Relation.createInRelation($name.id); }
       '(' ( f1=term { rel.addInValue(f1); } (',' fN=term { rel.addInValue(fN); } )* )? ')' { $clauses.add(rel); }
    | {
         List<ColumnIdentifier> ids = new ArrayList<ColumnIdentifier>();
         List<Term.Raw> terms = new ArrayList<Term.Raw>();
      }
        '(' n1=cident { ids.add(n1); } (',' ni=cident { ids.add(ni); })* ')'
        type=relationType
        '(' t1=term { terms.add(t1); } (',' ti=term { terms.add(ti); })* ')'
      {
          if (type == Relation.Type.IN)
              addRecognitionError("Cannot use IN relation with tuple notation");
          if (ids.size() != terms.size())
              addRecognitionError(String.format("Number of values (" + terms.size() + ") in tuple notation doesn't match the number of column names (" + ids.size() + ")"));
          else
              for (int i = 0; i < ids.size(); i++)
                  $clauses.add(new Relation(ids.get(i), type, terms.get(i), i == 0 ? null : ids.get(i-1)));
      }
    | '(' relation[$clauses] ')'
    ;

comparatorType returns [CQL3Type t]
    : c=native_type     { $t = c; }
    | c=collection_type { $t = c; }
    | s=STRING_LITERAL
      {
        try {
            $t = new CQL3Type.Custom($s.text);
        } catch (SyntaxException e) {
            addRecognitionError("Cannot parse type " + $s.text + ": " + e.getMessage());
        } catch (ConfigurationException e) {
            addRecognitionError("Error setting type " + $s.text + ": " + e.getMessage());
        }
      }
    ;

native_type returns [CQL3Type t]
    : K_ASCII     { $t = CQL3Type.Native.ASCII; }
    | K_BIGINT    { $t = CQL3Type.Native.BIGINT; }
    | K_BLOB      { $t = CQL3Type.Native.BLOB; }
    | K_BOOLEAN   { $t = CQL3Type.Native.BOOLEAN; }
    | K_COUNTER   { $t = CQL3Type.Native.COUNTER; }
    | K_DECIMAL   { $t = CQL3Type.Native.DECIMAL; }
    | K_DOUBLE    { $t = CQL3Type.Native.DOUBLE; }
    | K_FLOAT     { $t = CQL3Type.Native.FLOAT; }
    | K_INET      { $t = CQL3Type.Native.INET;}
    | K_INT       { $t = CQL3Type.Native.INT; }
    | K_TEXT      { $t = CQL3Type.Native.TEXT; }
    | K_TIMESTAMP { $t = CQL3Type.Native.TIMESTAMP; }
    | K_UUID      { $t = CQL3Type.Native.UUID; }
    | K_VARCHAR   { $t = CQL3Type.Native.VARCHAR; }
    | K_VARINT    { $t = CQL3Type.Native.VARINT; }
    | K_TIMEUUID  { $t = CQL3Type.Native.TIMEUUID; }
    ;

collection_type returns [CQL3Type pt]
    : K_MAP  '<' t1=comparatorType ',' t2=comparatorType '>'
        { try {
            // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
            if (t1 != null && t2 != null)
                $pt = CQL3Type.Collection.map(t1, t2);
          } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } }
    | K_LIST '<' t=comparatorType '>'
        { try { if (t != null) $pt = CQL3Type.Collection.list(t); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } }
    | K_SET  '<' t=comparatorType '>'
        { try { if (t != null) $pt = CQL3Type.Collection.set(t); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } }
    ;

username
    : IDENT
    | STRING_LITERAL
    ;

unreserved_keyword returns [String str]
    : u=unreserved_function_keyword     { $str = u; }
    | k=(K_TTL | K_COUNT | K_WRITETIME) { $str = $k.text; }
    ;

unreserved_function_keyword returns [String str]
    : k=( K_KEY
        | K_AS
        | K_CLUSTERING
        | K_COMPACT
        | K_STORAGE
        | K_TYPE
        | K_VALUES
        | K_MAP
        | K_LIST
        | K_FILTERING
        | K_PERMISSION
        | K_PERMISSIONS
        | K_KEYSPACES
        | K_ALL
        | K_USER
        | K_USERS
        | K_SUPERUSER
        | K_NOSUPERUSER
        | K_PASSWORD
        | K_EXISTS
        | K_CUSTOM
        | K_TRIGGER
        | K_DISTINCT
        | K_STATIC
        ) { $str = $k.text; }
    | t=native_type { $str = t.toString(); }
    ;


// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_AS:          A S;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_INSERT:      I N S E R T;
K_UPDATE:      U P D A T E;
K_WITH:        W I T H;
K_LIMIT:       L I M I T;
K_USING:       U S I N G;
K_USE:         U S E;
K_DISTINCT:    D I S T I N C T;
K_COUNT:       C O U N T;
K_SET:         S E T;
K_BEGIN:       B E G I N;
K_UNLOGGED:    U N L O G G E D;
K_BATCH:       B A T C H;
K_APPLY:       A P P L Y;
K_TRUNCATE:    T R U N C A T E;
K_DELETE:      D E L E T E;
K_IN:          I N;
K_CREATE:      C R E A T E;
K_KEYSPACE:    ( K E Y S P A C E
                 | S C H E M A );
K_KEYSPACES:   K E Y S P A C E S;
K_COLUMNFAMILY:( C O L U M N F A M I L Y
                 | T A B L E );
K_INDEX:       I N D E X;
K_CUSTOM:      C U S T O M;
K_ON:          O N;
K_TO:          T O;
K_DROP:        D R O P;
K_PRIMARY:     P R I M A R Y;
K_INTO:        I N T O;
K_VALUES:      V A L U E S;
K_TIMESTAMP:   T I M E S T A M P;
K_TTL:         T T L;
K_ALTER:       A L T E R;
K_RENAME:      R E N A M E;
K_ADD:         A D D;
K_TYPE:        T Y P E;
K_COMPACT:     C O M P A C T;
K_STORAGE:     S T O R A G E;
K_ORDER:       O R D E R;
K_BY:          B Y;
K_ASC:         A S C;
K_DESC:        D E S C;
K_ALLOW:       A L L O W;
K_FILTERING:   F I L T E R I N G;
K_IF:          I F;

K_GRANT:       G R A N T;
K_ALL:         A L L;
K_PERMISSION:  P E R M I S S I O N;
K_PERMISSIONS: P E R M I S S I O N S;
K_OF:          O F;
K_REVOKE:      R E V O K E;
K_MODIFY:      M O D I F Y;
K_AUTHORIZE:   A U T H O R I Z E;
K_NORECURSIVE: N O R E C U R S I V E;

K_USER:        U S E R;
K_USERS:       U S E R S;
K_SUPERUSER:   S U P E R U S E R;
K_NOSUPERUSER: N O S U P E R U S E R;
K_PASSWORD:    P A S S W O R D;

K_CLUSTERING:  C L U S T E R I N G;
K_ASCII:       A S C I I;
K_BIGINT:      B I G I N T;
K_BLOB:        B L O B;
K_BOOLEAN:     B O O L E A N;
K_COUNTER:     C O U N T E R;
K_DECIMAL:     D E C I M A L;
K_DOUBLE:      D O U B L E;
K_FLOAT:       F L O A T;
K_INET:        I N E T;
K_INT:         I N T;
K_TEXT:        T E X T;
K_UUID:        U U I D;
K_VARCHAR:     V A R C H A R;
K_VARINT:      V A R I N T;
K_TIMEUUID:    T I M E U U I D;
K_TOKEN:       T O K E N;
K_WRITETIME:   W R I T E T I M E;

K_NULL:        N U L L;
K_NOT:         N O T;
K_EXISTS:      E X I S T S;

K_MAP:         M A P;
K_LIST:        L I S T;
K_NAN:         N A N;
K_INFINITY:    I N F I N I T Y;

K_TRIGGER:     T R I G G E R;
K_STATIC:      S T A T I C;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');

STRING_LITERAL
    @init{ StringBuilder b = new StringBuilder(); }
    @after{ setText(b.toString()); }
    : '\'' (c=~('\'') { b.appendCodePoint(c);} | '\'' '\'' { b.appendCodePoint('\''); })* '\''
    ;

QUOTED_NAME
    @init{ StringBuilder b = new StringBuilder(); }
    @after{ setText(b.toString()); }
    : '\"' (c=~('\"') { b.appendCodePoint(c); } | '\"' '\"' { b.appendCodePoint('\"'); })+ '\"'
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;

fragment HEX
    : ('A'..'F' | 'a'..'f' | '0'..'9')
    ;

fragment EXPONENT
    : E ('+' | '-')? DIGIT+
    ;

INTEGER
    : '-'? DIGIT+
    ;

QMARK
    : '?'
    ;

/*
 * Normally a lexer only emits one token at a time, but ours is tricked out
 * to support multiple (see @lexer::members near the top of the grammar).
 */
FLOAT
    : INTEGER EXPONENT
    | INTEGER '.' DIGIT* EXPONENT?
    ;

/*
 * This has to be before IDENT so it takes precendence over it.
 */
BOOLEAN
    : T R U E | F A L S E
    ;

IDENT
    : LETTER (LETTER | DIGIT | '_')*
    ;

HEXNUMBER
    : '0' X HEX*
    ;

UUID
    : HEX HEX HEX HEX HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel = HIDDEN; }
    ;

COMMENT
    : ('--' | '//') .* ('\n'|'\r') { $channel = HIDDEN; }
    ;

MULTILINE_COMMENT
    : '/*' .* '*/' { $channel = HIDDEN; }
    ;
