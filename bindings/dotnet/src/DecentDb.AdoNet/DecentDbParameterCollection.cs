using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace DecentDb.AdoNet
{
    internal sealed class DecentDbParameterCollection : DbParameterCollection
    {
        private readonly List<DecentDbParameter> _parameters;

        public DecentDbParameterCollection(List<DecentDbParameter> parameters)
        {
            _parameters = parameters;
        }

        public override int Count => _parameters.Count;

        public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

        public override int Add(object value)
        {
            if (value is not DecentDbParameter p)
            {
                throw new ArgumentException("Parameter must be a DecentDbParameter", nameof(value));
            }
            _parameters.Add(p);
            return _parameters.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var v in values)
            {
                Add(v!);
            }
        }

        public override void Clear() => _parameters.Clear();

        public override bool Contains(object value) => value is DecentDbParameter p && _parameters.Contains(p);

        public override bool Contains(string? value) => IndexOf(value) >= 0;

        public override void CopyTo(Array array, int index) => ((ICollection)_parameters).CopyTo(array, index);

        public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

        public override int IndexOf(object value) => value is DecentDbParameter p ? _parameters.IndexOf(p) : -1;

        public override int IndexOf(string? parameterName)
        {
            if (parameterName == null) return -1;
            for (int i = 0; i < _parameters.Count; i++)
            {
                if (string.Equals(_parameters[i].ParameterName, parameterName, StringComparison.Ordinal))
                {
                    return i;
                }
            }
            return -1;
        }

        public override void Insert(int index, object value)
        {
            if (value is not DecentDbParameter p)
            {
                throw new ArgumentException("Parameter must be a DecentDbParameter", nameof(value));
            }
            _parameters.Insert(index, p);
        }

        public override void Remove(object value)
        {
            if (value is DecentDbParameter p)
            {
                _parameters.Remove(p);
            }
        }

        public override void RemoveAt(int index) => _parameters.RemoveAt(index);

        public override void RemoveAt(string parameterName)
        {
            var idx = IndexOf(parameterName);
            if (idx >= 0)
            {
                _parameters.RemoveAt(idx);
            }
        }

        protected override DbParameter GetParameter(int index) => _parameters[index];

        protected override DbParameter GetParameter(string parameterName)
        {
            var idx = IndexOf(parameterName);
            if (idx < 0)
            {
                throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found");
            }
            return _parameters[idx];
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            if (value is not DecentDbParameter p)
            {
                throw new ArgumentException("Parameter must be a DecentDbParameter", nameof(value));
            }
            _parameters[index] = p;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            if (value is not DecentDbParameter p)
            {
                throw new ArgumentException("Parameter must be a DecentDbParameter", nameof(value));
            }

            var idx = IndexOf(parameterName);
            if (idx < 0)
            {
                _parameters.Add(p);
            }
            else
            {
                _parameters[idx] = p;
            }
        }
    }
}
