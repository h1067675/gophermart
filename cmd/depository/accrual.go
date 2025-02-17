package depository

import (
	"database/sql"
	"errors"
	"strconv"

	"github.com/h1067675/gophermart/internal/logger"
)

type AccrualResponse struct {
	Order   string  `json:"order"`
	Status  string  `json:"status"`
	Accrual float64 `json:"accrual"`
}

func (s *Storage) OrderAccrual(order int) (res AccrualResponse, err error) {
	row := s.DB.QueryRow("SELECT order_number, status, sum FROM accrual_orders WHERE order_number = $1;", order)
	err = row.Scan(&res.Order, &res.Status, &res.Accrual)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			logger.Log.WithError(err).Info("error insert new order into db")
			return
		}
		res.Order = strconv.Itoa(order)
		switch order % 7 {
		case 1:
			res.Status = "REGISTERED"
		case 2:
			res.Status = "INVALID"
		case 3:
			res.Status = "PROCESSING"
		default:
			res.Status = "PROCESSED"
			res.Accrual = float64(order % 900)
		}
		_, err = s.DB.Exec("INSERT INTO accrual_orders (order_number, status, sum) VALUES ($1, $2, $3);", order, res.Status, res.Accrual)
		if err != nil {
			logger.Log.WithError(err).Error("error inserting order to orders")
			return
		}

	}

	return
}
