using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace consumer.Models
{
    public class Item
    {
        public string Name { get; set; }
        public string LotNum { get; set; }
        public int Qty { get; set; }
        public int Dose { get; set; }
        public string DoseMeasurement { get; set; }
    }
}
