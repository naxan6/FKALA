using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Migrations.Influx
{
    public class InfluxLineParser
    {
        public string? measurement = null;
        public List<(string, string)> tags = new List<(string, string)>();
        public List<(string, string)> fields = new List<(string, string)>();
        public DateTime? time = null;

        private List<char> sb = new List<char>();
        public InfluxLineParser() { }


        private void ReadMeasurement(IEnumerator<char> inputEnum)
        {
            sb.Clear();
            bool someleft = true;
            char c = ' ';
            while (someleft)
            {
                c = inputEnum.Current;
                if (',' != c)
                {
                    sb.Add(c);
                }
                else
                {
                    break;
                }
                someleft = inputEnum.MoveNext();
            }
            if (!someleft) { throw new Exception("ended too soon 1"); }

            this.measurement = new string(sb.ToArray()).Trim();
        }
        private void EatNonsense(IEnumerator<char> ce)
        {
            bool someleft = true;
            char c = ' ';
            while (someleft)
            {
                c = ce.Current;
                if (' ' == c || ',' == c)
                {
                    //eat;
                }
                else
                {
                    return;
                }
                someleft = ce.MoveNext();
            }
            if (!someleft) { throw new Exception("ended too soon 1"); }
        }

        private void ReadTags(IEnumerator<char> ce)
        {
            sb.Clear();
            bool someleft = true;
            bool inString = false;
            char c = ' ';
            while (someleft)
            {
                c = ce.Current;
                if ('"' == c)
                {
                    inString = !inString;
                    someleft = ce.MoveNext();
                    continue;
                }
                if (inString)
                {
                    sb.Add(c);
                    someleft = ce.MoveNext();
                    continue;
                }
                if (',' == c)
                {
                    var pair = new string(sb.ToArray()).Trim().Split("=");
                    this.tags.Add(new(pair[0], pair[1]));
                    EatNonsense(ce);
                    sb.Clear();
                    continue;
                }
                if (' ' == c)
                {
                    var pair = new string(sb.ToArray()).Trim().Split("=");
                    this.tags.Add(new(pair[0], pair[1]));
                    EatNonsense(ce);
                    sb.Clear();
                    return;
                }
                if ('\\' == c) //read escaped char instantly
                {
                    sb.Add(c);
                    someleft = ce.MoveNext();
                    sb.Add(c);
                    continue;
                }


                sb.Add(c);

                someleft = ce.MoveNext();
            }
            if (!someleft) { throw new Exception("ended too soon 1"); }
        }

        private void ReadFields(IEnumerator<char> ce)
        {
            sb.Clear();
            bool someleft = true;
            bool inString = false;
            char c = ' ';
            while (someleft)
            {
                c = ce.Current;
                if ('"' == c)
                {
                    inString = !inString;
                    someleft = ce.MoveNext();
                    continue;
                }
                if (inString)
                {
                    sb.Add(c);
                    someleft = ce.MoveNext();
                    continue;
                }
                if (',' == c)
                {
                    var pair = new string(sb.ToArray()).Trim().Split("=");
                    this.tags.Add(new(pair[0], pair[1]));
                    EatNonsense(ce);
                    sb.Clear();
                    continue;
                }
                if (' ' == c)
                {
                    var pair = new string(sb.ToArray()).Trim().Split("=");
                    this.fields.Add(new(pair[0], pair[1]));
                    EatNonsense(ce);
                    sb.Clear();
                    return;
                }
                if ('\\' == c) //read escaped char instantly
                {
                    sb.Add(c);
                    someleft = ce.MoveNext();
                    sb.Add(c);
                    continue;
                }


                sb.Add(c);

                someleft = ce.MoveNext();
            }
            if (!someleft) { throw new Exception("ended too soon 1"); }
        }

        private void ReadTime(IEnumerator<char> ce)
        {
            sb.Clear();
            bool someleft = true;

            char c = ' ';
            while (someleft)
            {
                c = ce.Current;
                sb.Add(c);
                someleft = ce.MoveNext();
            }
            var timestamp = new string(sb.ToArray());
            DateTime parsedTimestamp = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            parsedTimestamp = parsedTimestamp.AddTicks(long.Parse(timestamp) / 100);
            this.time = parsedTimestamp;
        }

        private void Reset()
        {
            measurement = null;
            tags.Clear();
            fields.Clear();
            time = null;
        }

        public void Read(string input)
        {
            this.Reset();
            var ce = input.GetEnumerator();
            ce.MoveNext();
            this.ReadMeasurement(ce);
            this.EatNonsense(ce);
            this.ReadTags(ce);
            this.EatNonsense(ce);
            this.ReadFields(ce);
            this.EatNonsense(ce);
            this.ReadTime(ce);
        }
    }
}
