using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace SqlToolsLib
{
    public static class SqlTools
    {
        #region LOGGING

        public static Action<string, IEnumerable<(string Name, object Value)>> SqlLog { get; set; }
        public static Action<string, object> SelectLog { get; set; }
        public static Action<string, object> InsertLog { get; set; }
        public static Action<string, object, object> UpdateLog { get; set; }
        public static Action<string, object> DropLog { get; set; }

        #endregion LOGGING

        #region GENERAL-USE

        public static IDbCommand SqlCommand(
            this IDbConnection connection,
            IDbTransaction transaction, string sql, IEnumerable<(string, object)> parameters)
        {
            var command = connection.CreateCommand();

            if (string.IsNullOrEmpty(sql))
                throw new ArgumentException(nameof(sql));
            command.CommandText = sql;
            command.Transaction = transaction;

            if (parameters.Any())
            {
                parameters.Distinct().ToList().ForEach(p =>
                {
                    var cp = command.CreateParameter();
                    cp.ParameterName = p.Item1;
                    cp.Value = p.Item2;
                    command.Parameters.Add(cp);
                });
            }

            SqlLog?.Invoke(command.CommandText,
                command.Parameters.Cast<IDbDataParameter>().Select(p => (p.ParameterName, p.Value)));

            return command;
        }

        public static IDbCommand SqlCommand(
            this IDbConnection connection,
            IDbTransaction transaction, string sql, params (string, object)[] parameters)
            => connection.SqlCommand(transaction, sql, parameters.AsEnumerable());

        public static IDbCommand SqlCommand<TParameter>(
            this IDbConnection connection,
            IDbTransaction transaction, string sql, TParameter parameter,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            => connection.SqlCommand(transaction, sql, parameter.ToValueTuple(bindingFlags));

        public static int ExecuteNonQuery(
            this IDbConnection connection, IDbTransaction transaction, string sql)
            => connection.SqlCommand(transaction, sql).ExecuteNonQuery();

        public static int ExecuteNonQuery<TParameter>(
            this IDbConnection connection, IDbTransaction transaction, string sql, TParameter parameters)
            => connection.SqlCommand(transaction, sql, parameters).ExecuteNonQuery();

        public static IDataReader ExecuteReader(
            this IDbConnection connection, IDbTransaction transaction, string sql)
            => connection.SqlCommand(transaction, sql).ExecuteReader();

        public static IDataReader ExecuteReader<TParameter>(
            this IDbConnection connection, IDbTransaction transaction, string sql, TParameter parameters)
            => connection.SqlCommand(transaction, sql, parameters).ExecuteReader();

        public static object ExecuteScalar(
            this IDbConnection connection, IDbTransaction transaction, string sql)
            => connection.SqlCommand(transaction, sql).ExecuteScalar();

        public static object ExecuteScalar<TParameter>(
            this IDbConnection connection, IDbTransaction transaction, string sql, TParameter parameters)
            => connection.SqlCommand(transaction, sql, parameters).ExecuteScalar();

        #endregion GENERAL-USE

        #region SELECT

        public static IDbCommand SelectCommand<TKey, TSelect>(
            this IDbConnection connection,
            string tableName, TKey key, TSelect select,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
        {
            var keys = key.ToValueTuple(bindingFlags);
            var keySql = keys.ToWhereString();

            var selects = select.ToValueTuple(bindingFlags);
            var selectSql = selects.ToColumnsString();
            if (string.IsNullOrEmpty(selectSql))
                selectSql = "*";

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException(nameof(tableName));

            var sql = $"select {selectSql} from {tableName}{keySql}";

            SelectLog?.Invoke(sql, key);

            return connection.SqlCommand(null, sql, keys.Concat(selects).Distinct().ToArray());
        }

        public static IDataReader ExecuteSelect<TKey, TSelect>(
            this IDbConnection connection, string tableName, TKey key, TSelect select,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            => connection.SelectCommand(tableName, key, select, bindingFlags).ExecuteReader();

        #endregion SELECT

        #region UPDATE

        public static IDbCommand UpdateCommand<TKey, TUpdate>(
            this IDbConnection connection,
            IDbTransaction transaction, string tableName, TKey key, TUpdate update,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
        {
            var updates = update.ToValueTuple(bindingFlags);
            if (!updates.Any())
                throw new ArgumentException(nameof(update));
            var updateSql = updates.ToUpdateSetString();

            var keys = key.ToValueTuple(bindingFlags);
            var keySql = keys.ToWhereString();

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException(nameof(tableName));
            var sql = $"update {tableName} set {updateSql}{keySql};";

            UpdateLog?.Invoke(sql, update, key);

            return connection.SqlCommand(transaction, sql, updates.Concat(keys).Distinct().ToArray());
        }

        public static int ExecuteUpdate<TKey, TUpdate>(
            this IDbConnection connection, IDbTransaction transaction, string tableName, TKey key, TUpdate update,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            => connection.UpdateCommand(transaction, tableName, key, update, bindingFlags).ExecuteNonQuery();

        #endregion UPDATE

        #region INSERT

        public static IDbCommand InsertCommand<TParameter>(
            this IDbConnection connection,
            IDbTransaction transaction, string tableName, TParameter parameter,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
        {
            var parameters = parameter.ToValueTuple(bindingFlags);
            if (!parameters.Any())
                throw new ArgumentException(nameof(parameter));

            var columns = parameters.ToColumnsString();
            var values = parameters.ToParametersString();

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException(nameof(tableName));
            var sql = $"insert into {tableName} ({columns}) values ({values});";

            InsertLog?.Invoke(sql, parameter);

            return connection.SqlCommand(transaction, sql, parameters.ToArray());
        }

        public static int ExecuteInsert<TParameter>(
            this IDbConnection connection, IDbTransaction transaction, string tableName, TParameter parameter,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            => connection.InsertCommand(transaction, tableName, parameter, bindingFlags).ExecuteNonQuery();

        #endregion INSERT

        #region DELETE

        public static IDbCommand DeleteCommand<TKey>(
            this IDbConnection connection,
            IDbTransaction transaction, string tableName, TKey key,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
        {
            var keys = key.ToValueTuple(bindingFlags);
            var keySql = keys.ToWhereString();

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException(nameof(tableName));
            var sql = $"delete from {tableName}{keySql};";

            DropLog?.Invoke(sql, key);

            return connection.SqlCommand(transaction, sql, keys.ToArray());
        }

        public static int ExecuteDelete<TKey>(
            this IDbConnection connection, IDbTransaction transaction, string tableName, TKey key,
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            => connection.DeleteCommand(transaction, tableName, key, bindingFlags).ExecuteNonQuery();

        #endregion DELETE

        #region (PRIVATE)

        private static IEnumerable<(string Name, object Value)> ToValueTuple<T>(this T obj, BindingFlags bindingFlags)
            => typeof(T).GetProperties(bindingFlags).Select(p => (p.Name, p.GetValue(obj)));

        private static string ToUpdateSetString(this IEnumerable<(string Name, object Value)> elements)
            => string.Join(", ", elements.Select(elem => $"{elem.Name} = :{elem.Name}"));

        private static string ToWhereString(this IEnumerable<(string Name, object Value)> elements)
        {
            var sql = string.Join(" and ", elements.Select(elem => $"{elem.Name} = :{elem.Name}"));

            if (!string.IsNullOrEmpty(sql))
            {
                sql = $" where {sql}";
            }

            return sql;
        }

        private static string ToColumnsString(this IEnumerable<(string Name, object Value)> elements)
            => string.Join(", ", elements.Select(elem => elem.Name));

        private static string ToParametersString(this IEnumerable<(string Name, object Value)> elements)
            => string.Join(", ", elements.Select(elem => $":{elem.Name}"));

        #endregion (PRIVATE)
    }
}