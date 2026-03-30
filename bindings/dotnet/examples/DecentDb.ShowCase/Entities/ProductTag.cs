using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DecentDb.ShowCase.Entities;

public class ProductTag
{
    [Key]
    public long ProductId { get; set; }

    [Key]
    public int TagId { get; set; }

    public DateTime AssignedAt { get; set; }
}
