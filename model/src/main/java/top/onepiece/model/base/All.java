package top.onepiece.model.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @author: zhouwubiao@bjca.org.cn
 * @date: Create in  2022/5/19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class All {
  private Request request;
  private Response response;
}
