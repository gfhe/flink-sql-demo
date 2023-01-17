package win.hgfdodo.datagenerator.repositories;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import win.hgfdodo.datagenerator.domain.Order;

@Repository
public interface OrderRepository extends CrudRepository<Order, Integer> {
}
