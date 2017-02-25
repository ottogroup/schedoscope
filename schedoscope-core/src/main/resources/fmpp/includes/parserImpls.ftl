<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/** Parses an STRUCT constructor */
SqlNode StructConstructor() :
{
    SqlNodeList args;
    SqlNode e;
    SqlParserPos pos;
}
{
    (
        <NAMED_STRUCT> { pos = getPos(); }
        // by named enumeration "named_struct(n0, v0, ..., nN, vN)"
        <LPAREN>
        (
            args = ExpressionCommaList(pos, ExprContext.ACCEPT_NONQUERY)
        |
            { args = new SqlNodeList(getPos()); }
        )
        <RPAREN>
        {
            return HiveQlOperatorTable.NAMED_STRUCT_VALUE_CONSTRUCTOR().createCall(
                pos.plus(getPos()), SqlParserUtil.toNodeArray(args));
        }
    |
        <STRUCT> { pos = getPos(); }
        // by enumeration "struct(v0, v1, ..., vN)"
        <LPAREN>
        (
            args = ExpressionCommaList(pos, ExprContext.ACCEPT_NONQUERY)
        |
            { args = new SqlNodeList(getPos()); }
        )
        <RPAREN>
        {
            return HiveQlOperatorTable.STRUCT_VALUE_CONSTRUCTOR().createCall(
                pos.plus(getPos()), SqlParserUtil.toNodeArray(args));
        }
    )
}