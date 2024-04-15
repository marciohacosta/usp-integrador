namespace ProjIntegrador.Domain.Model
{
    public class Temperature
    {
        #region Properties

        public int RegId { get; set; }

        public DateTime Data {  get; set; }

        public double TempMin { get; set; }

        public double TempMax { get; set; }

        public double TempMed {  get; set; }

        #endregion
    }
}
