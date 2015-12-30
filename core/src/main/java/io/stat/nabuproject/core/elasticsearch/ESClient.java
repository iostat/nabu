package io.stat.nabuproject.core.elasticsearch;

import io.stat.nabuproject.core.Component;
import org.elasticsearch.client.Client;

/**
 * Created by io on 12/26/15. io is an asshole for
 * not giving writing documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class ESClient extends Component {
    public abstract Client getESClient();
}
