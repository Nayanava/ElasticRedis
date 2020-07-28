using System;
using System.Collections.Generic;
using System.Text;

namespace RedisShardingBundle
{
    class DataMigrationMeta
    {
        public int OldClusterCount { get; set; }
        public int NewClusterCount { get; set; }
        public bool CutOver { get; set; }
        public long CutOverTimestampInMillis { get; set; }
    }
}
