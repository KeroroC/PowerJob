package tech.powerjob.server.persistence.remote.model;

import lombok.Data;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

import javax.persistence.*;
import java.util.Date;

/**
 * 用户信息表
 *
 * @author tjq
 * @since 2020/4/12
 */
@Data
@Entity
@Table(indexes = {
        @Index(name = "uidx01_user_info", columnList = "username"),
        @Index(name = "uidx02_user_info", columnList = "email")
})
public class UserInfoDO {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "native")
    @GenericGenerator(name = "native", strategy = "native", parameters = {
            @Parameter(name = "sequence_name", value = "USER_INFO_SEQ")
    })
    private Long id;

    private String username;

    private String password;
    /**
     * 手机号
     */
    private String phone;
    /**
     * 邮箱地址
     */
    private String email;
    /**
     * webHook
     */
    private String webHook;
    /**
     * 扩展字段
     */
    private String extra;

    private Date gmtCreate;

    private Date gmtModified;
}
