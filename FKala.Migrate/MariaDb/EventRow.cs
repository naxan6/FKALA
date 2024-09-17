namespace FKala.Migrate.MariaDb
{
    public class EventRow
    {
        public required string sensorName;
        public required string sensorPath;
        public required int? id;
        public required long timestamp;
        public required long? timeValue1;
        public required long? timeValue2;
        public required int? intValue1;
        public required int? intValue2;
        public required double? doubleValue1;
        public required double? doubleValue2;
        public required string? stringValue1;
        public required string? stringValue2;

        public EventRow() { }

        public override string ToString()
        {
            return
            sensorName + " # " +
            sensorPath + " # " +
            timestamp + " # " +
            timeValue1 + " # " +
            timeValue2 + " # " +
            intValue1 + " # " +
            intValue2 + " # " +
            doubleValue1 + " # " +
            doubleValue2 + " # " +
            stringValue1 + " # " +
            stringValue2;
        }
    }
}
